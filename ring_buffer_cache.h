#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "envoy/buffer/buffer.h"
#include "envoy/event/dispatcher.h"
#include "envoy/http/filter.h"
#include "envoy/http/header_map.h"
#include "source/common/buffer/buffer_impl.h"
#include "absl/synchronization/mutex.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace RingBufferCache {

class RingBufferCacheFilter;

// 3 simple states for each URL in our cache
enum class CacheState {
  Empty,     // not cached yet; the first GET request will go to the backend
  Fetching,  // primary request is currently downloading the response from the backend
  Cached     // download finished; full or ring-buffered response is stored in RAM
};

// a waiting "follower" request paused during request coalescing
struct CoalescedListener {
  RingBufferCacheFilter* filter{nullptr};   // pointer to the follower's filter instance
  Event::Dispatcher* dispatcher{nullptr};   // pointer to the follower's worker thread event loop
  std::shared_ptr<std::atomic<bool>> alive; // shared flag: true while request is open, false after onDestroy()
};

// represents ONE cached URL (holds its headers, std::deque ring buffer, and waiting listeners)
struct CachedResponse {
  explicit CachedResponse(uint64_t max_size) : max_buffer_size(max_size) {}

  // per-URL lock so reading/writing one URL never blocks other URLs in the cache
  absl::Mutex entry_mutex;

  // current state of this URL (starts as Empty). guarded by entry_mutex
  CacheState state ABSL_GUARDED_BY(entry_mutex){CacheState::Empty};

  // saved HTTP response headers (key, value) from the backend
  std::vector<std::pair<std::string, std::string>> response_headers ABSL_GUARDED_BY(entry_mutex);

  // the RAM Ring Buffer: a double-ended queue of Envoy memory chunks
  std::deque<Buffer::OwnedImpl> chunks ABSL_GUARDED_BY(entry_mutex);
  uint64_t max_buffer_size;                                 // max bytes allowed in this ring buffer
  uint64_t current_size ABSL_GUARDED_BY(entry_mutex){0};    // current bytes stored across all chunks
  bool evicted_any ABSL_GUARDED_BY(entry_mutex){false};     // true if old chunks were popped due to overflow

  // list of paused coalesced requests currently waiting for chunks of this URL
  std::vector<CoalescedListener> listeners ABSL_GUARDED_BY(entry_mutex);

  // appends new data to the back of 'chunks' and pops oldest chunks from the front if over max_buffer_size
  void addDataLocked(const Buffer::Instance& data) ABSL_EXCLUSIVE_LOCKS_REQUIRED(entry_mutex);
};

using CachedResponseSharedPtr = std::shared_ptr<CachedResponse>;

// global RAM cache shared across all worker threads, mapping (Host + URL) -> CachedResponse
class GlobalRingBufferCache {
public:
  explicit GlobalRingBufferCache(uint64_t default_max_buffer_size) : default_max_buffer_size_(default_max_buffer_size) {}

  // looks up (or creates) the CachedResponse box for a given Host and URL
  CachedResponseSharedPtr get(const std::string& host, const std::string& url);
private:
  uint64_t default_max_buffer_size_;
  absl::Mutex mutex_; // protects cache_map_ during hash table lookups/insertions
  std::unordered_map<std::string, CachedResponseSharedPtr> cache_map_ ABSL_GUARDED_BY(mutex_);
};

using GlobalRingBufferCacheSharedPtr = std::shared_ptr<GlobalRingBufferCache>;

// HTTP filter instantiated by Envoy once per incoming HTTP request
class RingBufferCacheFilter : public Http::StreamFilter {
public:
  explicit RingBufferCacheFilter(GlobalRingBufferCacheSharedPtr cache) : cache_(std::move(cache)), alive_flag_(std::make_shared<std::atomic<bool>>(true)) {}

  // called by Envoy when the HTTP stream finishes or the client disconnects
  void onDestroy() override;

  // --- Decoder (Incoming Request: Client -> Envoy) ---

  // intercepts incoming HTTP request headers (where we check Host + URL and do HIT / COALESCED / MISS)
  Http::FilterHeadersStatus decodeHeaders(Http::RequestHeaderMap& headers, bool end_stream) override;
  
  // pass-through for request body data (not needed for GET requests)
  Http::FilterDataStatus decodeData(Buffer::Instance&, bool) override 
  {
    return Http::FilterDataStatus::Continue;
  }

  // pass-through for request trailers
  Http::FilterTrailersStatus decodeTrailers(Http::RequestTrailerMap&) override 
  {
    return Http::FilterTrailersStatus::Continue;
  }

  // envoy calls this at stream creation to give us the decoder_callbacks_ control handle
  void setDecoderFilterCallbacks(Http::StreamDecoderFilterCallbacks& callbacks) override 
  {
    decoder_callbacks_ = &callbacks;
  }

  // --- Encoder (Outgoing Response: Backend -> Envoy -> Client) ---

  // pass-through for 1xx (e.g., 100 Continue) informational headers
  Http::Filter1xxHeadersStatus encode1xxHeaders(Http::ResponseHeaderMap&) override 
  {
    return Http::Filter1xxHeadersStatus::Continue;
  }

  // intercepts backend response headers (saves 200 OK headers and broadcasts to coalesced listeners)
  Http::FilterHeadersStatus encodeHeaders(Http::ResponseHeaderMap& headers, bool end_stream) override;

  // intercepts backend response body chunks (pushes to ring buffer and streams to coalesced listeners)
  Http::FilterDataStatus encodeData(Buffer::Instance& data, bool end_stream) override;

  // pass-through for response trailers
  Http::FilterTrailersStatus encodeTrailers(Http::ResponseTrailerMap&) override 
  {
    return Http::FilterTrailersStatus::Continue;
  }

  // pass-through for HTTP/2 or HTTP/3 metadata frames
  Http::FilterMetadataStatus encodeMetadata(Http::MetadataMap&) override 
  {
    return Http::FilterMetadataStatus::Continue;
  }

  // required by interface; unused because we inject responses via decoder_callbacks_
  void setEncoderFilterCallbacks(Http::StreamEncoderFilterCallbacks&) override {}

  // helper methods executed on a follower's worker thread to push coalesced headers and data
  void deliverCoalescedHeaders(const std::vector<std::pair<std::string, std::string>>& headers_copy, bool end_stream);
  void deliverCoalescedData(const std::string& chunk_bytes, bool end_stream);

private:
  GlobalRingBufferCacheSharedPtr cache_;                           // pointer to the global cache warehouse
  Http::StreamDecoderFilterCallbacks* decoder_callbacks_{nullptr}; // Envoy handle to reply/pause this stream
  CachedResponseSharedPtr active_entry_{nullptr};                  // pointer to the URL entry for this request

  bool is_primary_fetcher_{false};                                 // true if this request went to the backend to populate the cache
  std::shared_ptr<std::atomic<bool>> alive_flag_;                  // flipped to false in onDestroy() for cross-thread safety
};

} // namespace RingBufferCache
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy