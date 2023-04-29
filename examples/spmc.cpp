#include "cool_queues/cool_queues.hpp"

#include <fmt/format.h>

#include <array>
#include <atomic>
#include <chrono>
#include <mutex>
#include <random>
#include <ratio>
#include <thread>

int main() {
  std::mutex mtx;
  auto log = [&mtx](const auto &value) {
    std::lock_guard lg{mtx};
    fmt::print("{}\n", value);
  };

  const int consumers_count = 10;
  const int messages_to_receive = 100'000;

  std::array<std::byte, 1024> memory_buffer;
  std::atomic_bool producer_started = false;
  std::atomic_int consumers_finished = 0;

  std::thread producer_thread{[&] {
    log("[producer] thread start");

    unsigned messages_count = 0;
    std::uint64_t bytes_written = 0;

    cool_q::producer producer{memory_buffer};

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> size_distribution(0, 100);
    std::uniform_int_distribution<char> char_distribution('a', 'z');

    producer_started.store(true);

    while (consumers_finished.load() != consumers_count) {

      std::uint64_t next_msg_size = size_distribution(gen);
      char c = char_distribution(gen);
      std::string msg(next_msg_size, c);

      producer.write(msg.size(), [&](std::span<std::byte> buffer) {
        std::copy((const std::byte *)msg.data(),
                  (const std::byte *)msg.data() + msg.size(), buffer.data());
      });
      ++messages_count;
      bytes_written += msg.size();

      if (messages_count % 10'000 == 0) {
        log(fmt::format("[producer] messages={}, bytes={}", messages_count,
                        bytes_written));
      }
      std::this_thread::sleep_for(std::chrono::microseconds{10});
    }

    log("[producer] thread end");
  }};

  while (!producer_started.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds{100});
  }

  std::vector<std::thread> consumer_threads;

  for (int i = 0; i < consumers_count; ++i) {
    consumer_threads.emplace_back([&, i] {
      log(fmt::format("[consumer {}] thread begin", i));

      std::array<std::byte, 1024> consumer_buffer;

      cool_q::consumer consumer{memory_buffer};
      std::uint64_t bytes_received = 0;
      unsigned polls = 0;
      std::array<unsigned, (int)cool_q::poll_event_type::new_data> results{};

      for (int received_messages = 0;
           received_messages < messages_to_receive;) {

        std::size_t read_size = 0;

        ++polls;
        auto result = consumer.poll([&](const auto &data) {
          read_size = data.size();
          bytes_received += data.size();
          std::copy(data.begin(), data.end(), consumer_buffer.begin());
        });

        ++results[(int)result];

        if (result != cool_q::poll_event_type::new_data) {
          continue;
        }

        std::span read_data{consumer_buffer.data(), read_size};

        for (std::span<std::byte> msg : cool_q::messages_range{read_data}) {
          (void)msg;
          ++received_messages;
        }

        if (received_messages % 10'000 == 0) {
          log(fmt::format("[consumer {}] messages={}, bytes={}", i,
                          received_messages, bytes_received));
        }
      }

      log(fmt::format(
          "[consumer {}] polls={}, no-new-data={}, new-data={}, lost-sync={}, "
          "interrupted={}",
          i, polls, results[(int)cool_q::poll_event_type::no_new_data],
          results[(int)cool_q::poll_event_type::new_data],
          results[(int)cool_q::poll_event_type::lost_sync],
          results[(int)cool_q::poll_event_type::interrupted]));

      consumers_finished.store(consumers_finished.load() + 1);

      log(fmt::format("[consumer {}] thread end", i));
    });
  }

  for (auto &th : consumer_threads) {
    th.join();
  }

  producer_thread.join();
}