#include "source/extensions/filters/http/ring_buffer_cache/ring_buffer_cache.h"

#include "envoy/registry/registry.h"
#include "envoy/server/filter_config.h"
#include "source/common/http/header_map_impl.h"
#include "source/common/protobuf/protobuf.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace RingBufferCache {

// =============================================================================
// 1. Ring Buffer Eviction Logic
// =============================================================================

void CachedResponse::addDataLocked(const Buffer::Instance& data) 
{
  const uint64_t new_data_size = data.length();

  if (new_data_size == 0) return;

  // create a new empty Buffer::OwnedImpl at the back of the deque and copy the chunk into it
  chunks.emplace_back();
  chunks.back().add(data);
  current_size += new_data_size;

  // ring buffer eviction loop: while total stored bytes exceed max_buffer_size,
  // subtract the oldest chunk's size and pop it off the front of the deque
  while (current_size > max_buffer_size && !chunks.empty()) 
  {
    current_size -= chunks.front().length();
    chunks.pop_front();
    evicted_any = true;
  }
}

// =============================================================================
// 2. Global Cache Lookup (Host + URL)
// =============================================================================

CachedResponseSharedPtr GlobalRingBufferCache::get(const std::string& host, const std::string& url) 
{
  // combine Host header and URL path to form the unique cache key
  std::string cache_key = host + url;

  // lock the global map_ mutex
  absl::MutexLock lock(&mutex_);

  // look up the key in the unordered_map. Operator [] inserts a nullptr entry if not found
  auto& entry_ptr = cache_map_[cache_key];

  if (entry_ptr == nullptr)
    // first time seeing this URL: allocate a new CachedResponse with the configured max_buffer_size
    entry_ptr = std::make_shared<CachedResponse>(default_max_buffer_size_);

  return entry_ptr;
}

// =============================================================================
// 3. HTTP Filter Lifecycle & Request Coalescing
// =============================================================================

void RingBufferCacheFilter::onDestroy() 
{
  // atomically mark this filter as dead
  alive_flag_->store(false);
  decoder_callbacks_ = nullptr;
}

Http::FilterHeadersStatus RingBufferCacheFilter::decodeHeaders(Http::RequestHeaderMap& headers, bool) 
{
  // only cache and coalesce HTTP GET requests; let POST/PUT/DELETE pass straight through
  if (headers.getMethodValue() != "GET")
    return Http::FilterHeadersStatus::Continue;

  const std::string host = std::string(headers.getHostValue());
  const std::string path = std::string(headers.getPathValue());

  // retrieve the shared CachedResponse entry for this Host + URL and lock its per-URL mutex
  active_entry_ = cache_->get(host, path);
  CachedResponse* entry = active_entry_.get();

  // lock this specific URL's mutex before checking or changing its state/chunks
  absl::MutexLock lock(&entry->entry_mutex);

  // --- CASE 1: CACHE HIT (Serve directly from RAM Ring Buffer) ---
  if (entry->state == CacheState::Cached) 
  {
    // create a brand-new HTTP response header map to send back to the client
    auto resp_headers = Http::ResponseHeaderMapImpl::create();

    // copy all saved backend headers (200 OK, content-type, etc.) into our response,
    // skipping old length/encoding headers because we will set the exact RAM buffer size next
    for (const auto& kv : entry->response_headers) 
    {
      if (kv.first == "content-length" || kv.first == "transfer-encoding") continue;

      resp_headers->addCopy(Http::LowerCaseString(kv.first), kv.second);
    }

    // set content-length to the exact byte count currently stored in our ring buffer
    // (in case older chunks were evicted) and mark the response with x-cache: HIT
    resp_headers->addCopy(Http::LowerCaseString("content-length"), std::to_string(entry->current_size));
    resp_headers->addCopy(Http::LowerCaseString("x-cache"), "HIT");

    // send the HTTP headers back to the client
    // if there are no body chunks (header_only == true), end_stream is true right here
    const bool header_only = entry->chunks.empty();
    decoder_callbacks_->encodeHeaders(std::move(resp_headers), header_only, "ring_buffer_cache_hit");

    // if there are body chunks in the std::deque ring buffer, combine them into body_buffer
    // and send them to the client with end_stream = true (meaning the response is finished)
    if (!header_only) 
    {
      Buffer::OwnedImpl body_buffer;

      for (const auto& chunk : entry->chunks)
        body_buffer.add(chunk);

      decoder_callbacks_->encodeData(body_buffer, true);
    }

    // StopIteration tells Envoy NOT to forward this request to the backend server
    return Http::FilterHeadersStatus::StopIteration;
  }

  // --- CASE 2: REQUEST COALESCING (another request is already downloading this URL) ---
  if (entry->state == CacheState::Fetching) 
  {
    // if the file is already larger than max_buffer_size and initial chunks were evicted,
    // we cannot serve the start of the file from RAM, so let this request go to the backend
    if (entry->evicted_any)
      return Http::FilterHeadersStatus::Continue;

    // if the primary request already received the 200 OK headers or early body chunks before this second request arrived,
    // send those saved headers and early chunks right now (with end_stream = false) so this follower is completely caught up
    if (!entry->response_headers.empty()) 
    {
      deliverCoalescedHeaders(entry->response_headers, false);

      for (const auto& chunk : entry->chunks)
        deliverCoalescedData(chunk.toString(), false);
    }

    // register this request on the waiting list (entry->listeners) with our worker thread's
    // dispatcher so future chunks arriving in encodeData() are streamed to us live
    entry->listeners.push_back(CoalescedListener{this, &decoder_callbacks_->dispatcher(), alive_flag_});

    // pause this request so Envoy does not send a duplicate request to the backend
    return Http::FilterHeadersStatus::StopIteration;
  }

  // --- CASE 3: CACHE MISS (First request -> becomes Primary Fetcher) ---
  entry->state = CacheState::Fetching;
  entry->response_headers.clear();
  entry->chunks.clear();
  entry->current_size = 0;
  entry->evicted_any = false;
  is_primary_fetcher_ = true;

  // continue tells Envoy to route this primary request to the backend origin server
  return Http::FilterHeadersStatus::Continue;
}

Http::FilterHeadersStatus RingBufferCacheFilter::encodeHeaders(Http::ResponseHeaderMap& headers, bool end_stream) 
{
  // only the primary fetcher populates the cache and broadcasts to coalesced followers
  if (!is_primary_fetcher_ || active_entry_ == nullptr)
    return Http::FilterHeadersStatus::Continue;

  CachedResponse* entry = active_entry_.get();

  // only cache HTTP 200 OK responses. if backend returns 404/500, reset state to Empty
  if (headers.getStatusValue() != "200") 
  {
    absl::MutexLock lock(&entry->entry_mutex);

    entry->state = CacheState::Empty;
    entry->listeners.clear();
    is_primary_fetcher_ = false;

    return Http::FilterHeadersStatus::Continue;
  }

  std::vector<std::pair<std::string, std::string>> headers_copy;
  std::vector<CoalescedListener> listeners_copy;

  {
    absl::MutexLock lock(&entry->entry_mutex);
    // copy all response headers into temp_headers first
    std::vector<std::pair<std::string, std::string>> temp_headers;

    headers.iterate([&temp_headers](const Http::HeaderEntry& header) -> Http::HeaderMap::Iterate {
      temp_headers.emplace_back(std::string(header.key().getStringView()), std::string(header.value().getStringView()));
      
      return Http::HeaderMap::Iterate::Continue;
    });

    entry->response_headers = std::move(temp_headers);
    headers_copy = entry->response_headers;
    listeners_copy = entry->listeners;

    // if this response has headers only and no body (end_stream == true), mark it Cached
    if (end_stream) 
    {
      entry->state = CacheState::Cached;
      entry->listeners.clear();
    }
  }

  // tag the primary fetcher's own response with X-Cache: MISS
  headers.addCopy(Http::LowerCaseString("x-cache"), "MISS");

  // broadcast the response headers to every waiting coalesced follower on their worker thread
  for (const auto& listener : listeners_copy) 
  {
    listener.dispatcher->post(
        [alive = listener.alive, filter = listener.filter, headers_copy, end_stream]() 
        {
          if (alive->load())
            filter->deliverCoalescedHeaders(headers_copy, end_stream);
        });
  }

  return Http::FilterHeadersStatus::Continue;
}

Http::FilterDataStatus RingBufferCacheFilter::encodeData(Buffer::Instance& data, bool end_stream) 
{
  // only the primary fetcher writes body chunks into the ring buffer and broadcasts them
  if (!is_primary_fetcher_ || active_entry_ == nullptr)
    return Http::FilterDataStatus::Continue;

  CachedResponse* entry = active_entry_.get();
  const std::string chunk_bytes = data.toString();
  std::vector<CoalescedListener> listeners_copy;

  {
    absl::MutexLock lock(&entry->entry_mutex);

    if (entry->state != CacheState::Fetching)
      return Http::FilterDataStatus::Continue;

    // push chunk into the std::deque ring buffer (and evict oldest chunks if over max_buffer_size)
    entry->addDataLocked(data);
    listeners_copy = entry->listeners;

    // if this is the final chunk of the response, transition state to Cached and clear listeners
    if (end_stream) 
    {
      entry->state = CacheState::Cached;
      entry->listeners.clear();
    }
  }

  // immediately stream this chunk to all waiting followers on their respective worker threads
  // so they receive data as soon as it is available (preventing Envoy idle timeouts)
  for (const auto& listener : listeners_copy) 
  {
    listener.dispatcher->post(
        [alive = listener.alive, filter = listener.filter, chunk_bytes, end_stream]() {
          if (alive->load())
            filter->deliverCoalescedData(chunk_bytes, end_stream);
        });
  }

  return Http::FilterDataStatus::Continue;
}

void RingBufferCacheFilter::deliverCoalescedHeaders(const std::vector<std::pair<std::string, std::string>>& headers_copy, bool end_stream) 
{
  // guard against sending headers twice (e.g., if catch-up and live broadcast overlap)
  if (decoder_callbacks_ == nullptr) return;

  auto resp_headers = Http::ResponseHeaderMapImpl::create();

  for (const auto& kv : headers_copy)
    resp_headers->addCopy(Http::LowerCaseString(kv.first), kv.second);

  resp_headers->addCopy(Http::LowerCaseString("x-cache"), "COALESCED");

  decoder_callbacks_->encodeHeaders(std::move(resp_headers), end_stream, "ring_buffer_cache_coalesced");
}

void RingBufferCacheFilter::deliverCoalescedData(const std::string& chunk_bytes, bool end_stream) 
{
  if (decoder_callbacks_ == nullptr) return;

  Buffer::OwnedImpl buf(chunk_bytes);
  decoder_callbacks_->encodeData(buf, end_stream);
}

// =============================================================================
// 4. Envoy Filter Registration (Reads max_buffer_size from envoy.yaml)
// =============================================================================

class RingBufferCacheConfigFactory : public Server::Configuration::NamedHttpFilterConfigFactory 
{
public:
  absl::StatusOr<Http::FilterFactoryCb> createFilterFactoryFromProto(const Protobuf::Message& proto_config, const std::string&,
      Server::Configuration::FactoryContext&) override 
    {
        uint64_t max_buffer_size = 1024 * 1024; // Default 1 MB

        // extract "max_buffer_size" from the google.protobuf.Struct in envoy.yaml
        if (const auto* struct_config = dynamic_cast<const Protobuf::Struct*>(&proto_config); struct_config != nullptr) 
        {
            const auto& fields = struct_config->fields();
            auto it = fields.find("max_buffer_size");

            if (it != fields.end() && it->second.has_number_value())
                max_buffer_size = static_cast<uint64_t>(it->second.number_value());
        }

        // create ONE shared GlobalRingBufferCache instance for the proxy
        auto shared_cache = std::make_shared<GlobalRingBufferCache>(max_buffer_size);

        // return a factory lambda that Envoy calls to create a RingBufferCacheFilter for each HTTP stream
        return [shared_cache](Http::FilterChainFactoryCallbacks& callbacks) -> void 
        {
            callbacks.addStreamFilter(std::make_shared<RingBufferCacheFilter>(shared_cache));
        };
    }

  ProtobufTypes::MessagePtr createEmptyConfigProto() override 
  {
    return std::make_unique<Protobuf::Struct>();
  }

  std::string name() const override { return "envoy.filters.http.ring_buffer_cache"; }
};

// registers RingBufferCacheConfigFactory with Envoy's global extension registry at startup
REGISTER_FACTORY(RingBufferCacheConfigFactory, Server::Configuration::NamedHttpFilterConfigFactory);

} // namespace RingBufferCache
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy