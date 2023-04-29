add_executable(cool_queues_benchmark
  cool_queues_benchmark.cpp)

target_link_libraries(cool_queues_benchmark cool_queues benchmark::benchmark)
