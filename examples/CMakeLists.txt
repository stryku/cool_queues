add_executable(cool_queues_hello_world_example
  hello_world.cpp)

target_link_libraries(cool_queues_hello_world_example cool_queues)

add_executable(cool_queues_spmc_example
  spmc.cpp)

target_link_libraries(cool_queues_spmc_example cool_queues)
