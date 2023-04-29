add_library(cool_queues STATIC
  cool_queues.hpp cool_queues.cpp
)

target_include_directories(cool_queues INTERFACE ..)
target_link_libraries(cool_queues fmt::fmt)