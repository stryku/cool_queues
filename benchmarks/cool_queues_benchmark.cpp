#include "cool_queues/cool_queues.hpp"

#include <benchmark/benchmark.h>

#include <array>
#include <random>

static void std_copy(benchmark::State &state) {
  std::vector<std::byte> memory_buffer(1024 * 1024 + 1024);
  std::array<std::byte, 512> message_buffer{};

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(0, 512);

  for (auto &byte : message_buffer) {
    byte = std::byte(dist(gen));
  }

  std::int64_t bytes_processed = 0;
  std::int64_t writes = 0;

  for (auto _ : state) {
    const auto size = dist(gen);
    std::copy(message_buffer.begin(), message_buffer.begin() + size,
              memory_buffer.begin());
    ++writes;
    bytes_processed += size;
    benchmark::DoNotOptimize(memory_buffer);
    benchmark::DoNotOptimize(message_buffer);
  }

  state.SetBytesProcessed(bytes_processed);
  state.SetItemsProcessed(writes);
}

static void producer(benchmark::State &state) {
  std::vector<std::byte> memory_buffer(1024 * 1024 + 1024);
  std::array<std::byte, 512> message_buffer{};

  cool_q::producer<1024 * 1024> producer{memory_buffer};

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(0, 512);

  for (auto &byte : message_buffer) {
    byte = std::byte(dist(gen));
  }

  std::int64_t bytes_processed = 0;
  std::int64_t writes = 0;

  for (auto _ : state) {
    const auto size = dist(gen);
    producer.write(size, [&](const auto buffer) {
      std::copy(message_buffer.begin(), message_buffer.begin() + size,
                buffer.begin());
    });
    ++writes;
    bytes_processed += size;
    benchmark::DoNotOptimize(memory_buffer);
    benchmark::DoNotOptimize(message_buffer);
  }

  state.SetBytesProcessed(bytes_processed);
  state.SetItemsProcessed(writes);
}

static void consumer(benchmark::State &state) {
  std::vector<std::byte> memory_buffer(1024 * 1024 + 1024);
  std::array<std::byte, 512> message_buffer{};
  std::array<std::byte, 1024 * 1024> consumer_buffer;

  cool_q::producer<1024 * 1024> producer{memory_buffer};

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(0, 512);

  for (auto &byte : message_buffer) {
    byte = std::byte(dist(gen));
  }

  std::uint64_t produced_bytes = 0;

  while (produced_bytes < consumer_buffer.size()) {
    const auto size = dist(gen);
    producer.write(size, [&](const auto buffer) {
      std::copy(message_buffer.begin(), message_buffer.begin() + size,
                buffer.begin());
    });
    produced_bytes += size;
  }

  std::int64_t bytes_processed = 0;
  std::int64_t messages = 0;

  for (auto _ : state) {
    cool_q::consumer consumer{memory_buffer};

    while (true) {
      std::uint64_t read_size = 0;

      const auto result = consumer.poll([&](const auto data) {
        std::copy(data.begin(), data.end(), consumer_buffer.begin());
        bytes_processed += data.size();
        read_size = data.size();
      });

      if (result == cool_q::consumer::poll_event_type::no_new_data) {
        break;
      }

      const std::span read_data{consumer_buffer.data(), read_size};

      for (auto msg : cool_q::messages_range{read_data}) {
        (void)msg;
        ++messages;
      }

      benchmark::DoNotOptimize(memory_buffer);
      benchmark::DoNotOptimize(message_buffer);
      benchmark::DoNotOptimize(consumer_buffer);
    }
  }

  state.SetBytesProcessed(bytes_processed);
  state.SetItemsProcessed(messages);
}

BENCHMARK(std_copy);
BENCHMARK(producer);
BENCHMARK(consumer);

BENCHMARK_MAIN();
