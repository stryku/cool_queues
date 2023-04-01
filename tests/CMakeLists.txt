enable_testing()

add_executable(cool_queues_test
  cool_queues_test.cpp)

target_link_libraries(cool_queues_test cool_queues gtest_main)
