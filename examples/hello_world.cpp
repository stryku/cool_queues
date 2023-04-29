#include "cool_queues/cool_queues.hpp"

#include <fmt/format.h>

#include <array>

int main() {

  std::array<std::byte, 1024> memory_buffer;

  cool_q::producer producer{memory_buffer};

  std::array<std::byte, 1024> consumer_buffer;
  cool_q::consumer consumer{memory_buffer};

  std::string_view hello = "Hello ";
  std::string_view world = "World!\n";

  producer.write(hello.size(), [&](auto buffer) {
    std::copy((const std::byte *)hello.data(),
              (const std::byte *)hello.data() + hello.size(), buffer.data());
  });

  producer.write(world.size(), [&](auto buffer) {
    std::copy((const std::byte *)world.data(),
              (const std::byte *)world.data() + world.size(), buffer.data());
  });

  std::size_t read_size = 0;

  consumer.poll([&](const auto &data) {
    read_size = data.size();
    std::copy(data.begin(), data.end(), consumer_buffer.begin());
  });

  std::span read_data{consumer_buffer.data(), read_size};

  for (auto msg : cool_q::messages_range{read_data}) {
    std::string_view msg_str{(const char *)msg.data(), msg.size()};
    fmt::print("{}", msg_str);
  }
}
