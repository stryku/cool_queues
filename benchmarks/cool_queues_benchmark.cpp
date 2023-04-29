#include "cool_queues/cool_queues.hpp"

#include <benchmark/benchmark.h>

#include <array>
#include <random>

static void std_copy(benchmark::State &state) {
  std::array<std::byte, 4096> memory_buffer;
  std::array<std::byte, 512> message_buffer{};

  cool_q::producer producer{memory_buffer};

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(0, 512);

  for (auto &byte : message_buffer) {
    byte = std::byte(dist(gen));
  }

  for (auto _ : state) {
    const auto size = dist(gen);
    std::copy(message_buffer.begin(), message_buffer.begin() + size,
              memory_buffer.begin());
    benchmark::DoNotOptimize(memory_buffer);
    benchmark::DoNotOptimize(message_buffer);
  }
}
BENCHMARK(std_copy);

static void producer(benchmark::State &state) {
  std::array<std::byte, 4096> memory_buffer;
  std::array<std::byte, 512> message_buffer{};

  cool_q::producer producer{memory_buffer};

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(0, 512);

  for (auto &byte : message_buffer) {
    byte = std::byte(dist(gen));
  }

  for (auto _ : state) {
    const auto size = dist(gen);
    producer.write(size, [&](const auto buffer) {
      std::copy(message_buffer.begin(), message_buffer.begin() + size,
                buffer.begin());
    });
    benchmark::DoNotOptimize(memory_buffer);
    benchmark::DoNotOptimize(message_buffer);
  }
}
BENCHMARK(producer);

BENCHMARK_MAIN();
