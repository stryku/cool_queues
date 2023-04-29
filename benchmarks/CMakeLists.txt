FetchContent_Declare(
  googlebenchmark
  GIT_REPOSITORY https://github.com/google/benchmark.git
  GIT_TAG v1.7.1)
FetchContent_MakeAvailable(googlebenchmark)

add_executable(cool_queues_benchmark
  cool_queues_benchmark.cpp)

target_link_libraries(cool_queues_benchmark cool_queues benchmark::benchmark)
