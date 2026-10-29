load(
    "//bazel:envoy_build_system.bzl",
    "envoy_cc_extension",
    "envoy_extension_package",
)

licenses(["notice"])  # Apache 2

envoy_extension_package()

envoy_cc_extension(
    name = "config",
    srcs = ["ring_buffer_cache.cc"],
    hdrs = ["ring_buffer_cache.h"],
    deps = [
        "//envoy/event:dispatcher_interface",
        "//envoy/http:filter_interface",
        "//envoy/http:header_map_interface",
        "//envoy/registry",
        "//envoy/server:filter_config_interface",
        "//source/common/buffer:buffer_lib",
        "//source/common/http:header_map_lib",
        "//source/common/protobuf:utility_lib",
    ],
)