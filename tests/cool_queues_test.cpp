// #define COOL_Q_PRODUCER_LOG(x) fmt::print("[producer] {}\n", (x))
// #define COOL_Q_CONSUMER_LOG(x) fmt::print("[consumer] {}\n", (x))

#include "cool_queues/cool_queues.hpp"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <limits>
#include <mutex>
#include <random>
#include <string>
#include <string_view>

namespace cool_q::test {

using namespace std::literals;

namespace {
[[maybe_unused]] constexpr std::array k_messages{"111111"sv,
                                                 "222222222"sv,
                                                 "333333333333"sv,
                                                 "444444444444444"sv,
                                                 "555555555"sv,
                                                 "66666"sv,
                                                 "7"sv,
                                                 "8888888888"sv,
                                                 "9999999999"sv,
                                                 "000000000000"sv};
}

struct test_consumer {
  test_consumer(std::span<std::byte> queue_buffer) : m_consumer{queue_buffer} {}
  consumer m_consumer;
  std::array<std::byte, 1024> m_buffer;
};

class MessagingTest : public ::testing::Test {
public:
  void SetUp() override {
    resetup(1024);
  }

  void TearDown() override {
    m_producer.reset();
    m_consumer.reset();
  }

  void resetup(std::uint64_t capacity) {

    m_memory_buffer.resize(capacity + sizeof(buffer_header) +
                           sizeof(buffer_footer));
    m_consumer_buffer.resize(capacity);

    std::span<std::byte> buffer = m_memory_buffer;
    m_producer = std::make_unique<producer>(buffer);
    m_consumer = std::make_unique<consumer>(buffer);
  }

  auto get_header() {
    return *(reinterpret_cast<buffer_header *>(m_memory_buffer.data()));
  }

  void fill_leaving_space(std::uint64_t space_to_leave) {
    auto header = get_header();
    auto space_left = header.m_capacity - header.m_end_offset;
    auto msg_size = space_left - sizeof(message_header) - space_to_leave;
    m_producer->write(msg_size, [](auto) {});
    consume_everything();
  }

  void consume_everything() {
    m_consumer->poll([](auto) {});
  }

  void write(std::string_view msg) {
    m_producer->write(msg.size(), [&](std::span<std::byte> buffer) {
      ASSERT_EQ(buffer.size(), msg.size());
      ASSERT_GE(buffer.data(), m_memory_buffer.data());
      ASSERT_LT(buffer.data() + buffer.size(),
                m_memory_buffer.data() + m_memory_buffer.size());
      std::memcpy(buffer.data(), msg.data(), msg.size());
    });
  }

  std::string make_signed_message(std::string_view msg) {
    const std::uint64_t checksum = std::hash<std::string_view>()(msg);

    std::string result(msg);
    result.resize(msg.size() + sizeof(checksum));

    std::memcpy(result.data() + msg.size(), &checksum, sizeof(checksum));

    return result;
  }

  bool validate_signed_message(std::string_view msg) {
    std::string_view msg_content =
        msg.substr(0, msg.size() - sizeof(std::uint64_t));
    std::uint64_t checksum;
    std::memcpy(&checksum, msg.data() + msg.size() - sizeof(std::uint64_t),
                sizeof(std::uint64_t));

    return std::hash<std::string_view>()(msg_content) == checksum;
  }

  std::vector<std::byte> m_memory_buffer;
  std::vector<std::byte> m_consumer_buffer;
  std::unique_ptr<producer> m_producer;
  std::unique_ptr<consumer> m_consumer;
};

TEST(BufferTest, Constructor) {
  std::array<std::byte, 1024> memory_buffer;

  std::fill(memory_buffer.begin(), memory_buffer.end(), std::byte{0xff});

  buffer buf(memory_buffer);
  auto &header = buf.access_header();
  EXPECT_EQ(header.m_header_size, sizeof(header));
  EXPECT_EQ(header.m_end_offset, 0);
  EXPECT_EQ(header.m_version, 0);
  EXPECT_EQ(header.m_capacity, memory_buffer.size() - sizeof(buffer_header) -
                                   sizeof(buffer_footer));

  header.m_end_offset = 40;
  header.m_version = 41;

  buffer buf2(memory_buffer, header);
  EXPECT_EQ(header.m_header_size, sizeof(header));
  EXPECT_EQ(header.m_end_offset, 40);
  EXPECT_EQ(header.m_version, 41);
  EXPECT_EQ(header.m_capacity, memory_buffer.size() - sizeof(buffer_header) -
                                   sizeof(buffer_footer));
}

TEST(ProducerTest, Constructor) {
  std::array<std::byte, 1024> memory_buffer;
  std::fill(memory_buffer.begin(), memory_buffer.end(), std::byte{0xff});

  producer producer{memory_buffer};

  buffer buffer(memory_buffer, buffer_header{});
  auto &header = buffer.access_header();
  EXPECT_EQ(header.m_header_size, sizeof(header));
  EXPECT_EQ(header.m_end_offset, 0);
  EXPECT_EQ(header.m_version, 0);
  EXPECT_EQ(header.m_capacity, memory_buffer.size() - sizeof(buffer_header) -
                                   sizeof(buffer_footer));
}

TEST(ConsumerTest, Constructor) {
  std::array<std::byte, 1024> memory_buffer;
  std::fill(memory_buffer.begin(), memory_buffer.end(), std::byte{0xff});

  producer producer{memory_buffer};

  buffer buffer(memory_buffer, buffer_header{});
  auto &header = buffer.access_header();
  header.m_end_offset = 40;
  header.m_version = 41;

  consumer consumer(memory_buffer);
  // Ensure it doesn't override header
  EXPECT_EQ(header.m_header_size, sizeof(header));
  EXPECT_EQ(header.m_end_offset, 40);
  EXPECT_EQ(header.m_version, 41);
  EXPECT_EQ(header.m_capacity, memory_buffer.size() - sizeof(buffer_header) -
                                   sizeof(buffer_footer));
}

TEST_F(MessagingTest, BasicTest) {

  std::string msg1 = "1111";
  std::string msg2 = "2222222";

  // Message 1
  m_producer->write(msg1.size(), [&](std::span<std::byte> buffer) {
    ASSERT_EQ(buffer.size(), msg1.size());
    std::memcpy(buffer.data(), msg1.data(), msg1.size());
  });

  std::uint64_t read_size = 0;

  auto result = m_consumer->poll([&](auto new_data) {
    read_size = new_data.size();
    std::memcpy(m_consumer_buffer.data(), new_data.data(), new_data.size());
  });

  ASSERT_EQ(result, consumer::poll_event_type::new_data);
  ASSERT_EQ(read_size, msg1.size() + sizeof(message_header));
  std::string_view read_msg{
      (const char *)(m_consumer_buffer.data() + sizeof(message_header)),
      read_size - sizeof(message_header)};
  EXPECT_EQ(read_msg, msg1);

  // Message 2
  m_producer->write(msg2.size(), [&](std::span<std::byte> buffer) {
    ASSERT_EQ(buffer.size(), msg2.size());
    std::memcpy(buffer.data(), msg2.data(), msg2.size());
  });

  result = m_consumer->poll([&](auto new_data) {
    read_size = new_data.size();
    std::memcpy(m_consumer_buffer.data(), new_data.data(), new_data.size());
  });

  ASSERT_EQ(result, consumer::poll_event_type::new_data);
  ASSERT_EQ(read_size, msg2.size() + sizeof(message_header));
  read_msg = std::string_view{
      (const char *)(m_consumer_buffer.data() + sizeof(message_header)),
      read_size - sizeof(message_header)};
  EXPECT_EQ(read_msg, msg2);

  // Message 1 and 2
  m_producer->write(msg1.size(), [&](std::span<std::byte> buffer) {
    ASSERT_EQ(buffer.size(), msg1.size());
    std::memcpy(buffer.data(), msg1.data(), msg1.size());
  });
  m_producer->write(msg2.size(), [&](std::span<std::byte> buffer) {
    ASSERT_EQ(buffer.size(), msg2.size());
    std::memcpy(buffer.data(), msg2.data(), msg2.size());
  });

  result = m_consumer->poll([&](auto new_data) {
    read_size = new_data.size();
    std::memcpy(m_consumer_buffer.data(), new_data.data(), new_data.size());
  });

  ASSERT_EQ(result, consumer::poll_event_type::new_data);
  ASSERT_EQ(read_size, 2 * sizeof(message_header) + msg1.size() + msg2.size());

  read_msg = std::string_view{
      (const char *)(m_consumer_buffer.data() + sizeof(message_header)),
      msg1.size()};
  EXPECT_EQ(read_msg, msg1);

  read_msg =
      std::string_view{(const char *)(m_consumer_buffer.data() +
                                      2 * sizeof(message_header) + msg1.size()),
                       msg2.size()};
  EXPECT_EQ(read_msg, msg2);
}

TEST_F(MessagingTest, MessagesRange) {

  for (auto msg : k_messages) {
    m_producer->write(msg.size(), [&](std::span<std::byte> buffer) {
      ASSERT_EQ(buffer.size(), msg.size());
      std::memcpy(buffer.data(), msg.data(), msg.size());
    });
  }

  std::uint64_t read_size = 0;

  auto result = m_consumer->poll([&](auto new_data) {
    read_size = new_data.size();
    std::memcpy(m_consumer_buffer.data(), new_data.data(), new_data.size());
  });

  ASSERT_EQ(result, consumer::poll_event_type::new_data);
  std::span<std::byte> read_data{m_consumer_buffer.data(), read_size};
  int i = 0;
  for (auto read_msg : messages_range{read_data}) {
    std::string_view read_str{(const char *)read_msg.data(), read_msg.size()};
    EXPECT_EQ(read_str, k_messages[i++]);
  }
}

TEST_F(MessagingTest, MultipleConsumers) {

  int n = 100;
  std::vector<test_consumer> consumers;
  for (int i = 0; i < n; ++i) {
    consumers.emplace_back(m_memory_buffer);
  }

  for (auto msg : k_messages) {
    m_producer->write(msg.size(), [&](std::span<std::byte> buffer) {
      ASSERT_EQ(buffer.size(), msg.size());
      std::memcpy(buffer.data(), msg.data(), msg.size());
    });
  }

  for (int i = 0; i < n; ++i) {
    auto &consumer = consumers[i];

    std::uint64_t read_size = 0;

    auto result = consumer.m_consumer.poll([&](auto new_data) {
      read_size = new_data.size();
      std::memcpy(consumer.m_buffer.data(), new_data.data(), new_data.size());
    });

    ASSERT_EQ(result, consumer::poll_event_type::new_data);
    std::span<std::byte> read_data{consumer.m_buffer.data(), read_size};
    int msg_i = 0;
    for (auto read_msg : messages_range{read_data}) {
      std::string_view read_str{(const char *)read_msg.data(), read_msg.size()};
      EXPECT_EQ(read_str, k_messages[msg_i++]);
    }
  }
}

TEST_F(MessagingTest, MultipleConsumersMultithread) {

  for (auto msg : k_messages) {
    m_producer->write(msg.size(), [&](std::span<std::byte> buffer) {
      ASSERT_EQ(buffer.size(), msg.size());
      std::memcpy(buffer.data(), msg.data(), msg.size());
    });
  }

  std::vector<std::thread> threads;

  int n = 30;
  for (int i = 0; i < n; ++i) {
    threads.emplace_back([&] {
      test_consumer consumer{m_memory_buffer};
      std::uint64_t read_size = 0;

      auto result = consumer.m_consumer.poll([&](auto new_data) {
        read_size = new_data.size();
        std::memcpy(consumer.m_buffer.data(), new_data.data(), new_data.size());
      });

      ASSERT_EQ(result, consumer::poll_event_type::new_data);
      std::span<std::byte> read_data{consumer.m_buffer.data(), read_size};
      int msg_i = 0;
      for (auto read_msg : messages_range{read_data}) {
        std::string_view read_str{(const char *)read_msg.data(),
                                  read_msg.size()};
        EXPECT_EQ(read_str, k_messages[msg_i++]);
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }
}

TEST_F(MessagingTest, MessagePerfectlyFills) {
  std::string msg(100, 'C');
  auto wrapped_message_size_with_header = msg.size() + sizeof(message_header);

  fill_leaving_space(wrapped_message_size_with_header);

  write(msg);

  std::uint64_t read_size = 0;

  auto result = m_consumer->poll([&](auto new_data) {
    read_size = new_data.size();
    std::memcpy(m_consumer_buffer.data(), new_data.data(), new_data.size());
  });

  ASSERT_EQ(result, consumer::poll_event_type::new_data);
  ASSERT_EQ(read_size, msg.size() + sizeof(message_header));
  std::string_view read_msg{
      (const char *)(m_consumer_buffer.data() + sizeof(message_header)),
      read_size - sizeof(message_header)};
  EXPECT_EQ(read_msg, msg);
}

TEST_F(MessagingTest, MessageOneByteTooBig) {
  std::string msg(100, 'C');
  auto wrapped_message_size_with_header = msg.size() + sizeof(message_header);

  fill_leaving_space(wrapped_message_size_with_header - 1);

  write(msg);

  std::uint64_t read_size = 0;

  auto result = m_consumer->poll([&](auto new_data) {
    read_size = new_data.size();
    std::memcpy(m_consumer_buffer.data(), new_data.data(), new_data.size());
  });

  ASSERT_EQ(result, consumer::poll_event_type::new_data);
  ASSERT_EQ(read_size, msg.size() + sizeof(message_header));
  std::string_view read_msg{
      (const char *)(m_consumer_buffer.data() + sizeof(message_header)),
      read_size - sizeof(message_header)};
  EXPECT_EQ(read_msg, msg);
}

TEST_F(MessagingTest, MessageOfExactlyQueueCapacity) {
  auto header = get_header();
  std::string msg(100, 'C');
  auto wrapped_message_size_with_header = msg.size() + sizeof(message_header);

  fill_leaving_space(wrapped_message_size_with_header - 1);

  msg = std::string(header.m_capacity - sizeof(message_header), 'c');

  write(msg);

  std::uint64_t read_size = 0;

  auto result = m_consumer->poll([&](auto new_data) {
    read_size = new_data.size();
    std::memcpy(m_consumer_buffer.data(), new_data.data(), new_data.size());
  });

  ASSERT_EQ(result, consumer::poll_event_type::new_data);
  ASSERT_EQ(read_size, msg.size() + sizeof(message_header));
  std::string_view read_msg{
      (const char *)(m_consumer_buffer.data() + sizeof(message_header)),
      read_size - sizeof(message_header)};
  EXPECT_EQ(read_msg, msg);
}

TEST_F(MessagingTest, MultipleMessagesFillQueueExactly) {

  resetup(100);

  // Fill up to 40
  fill_leaving_space(60);
  consume_everything();

  // Write 5 messages 20 bytes each (including message header). This should
  // fill whole Q without overrunning consumer
  const auto message_size = 20 - sizeof(message_header);

  for (char c = '0'; c < '5'; ++c) {
    auto msg = std::string(message_size, c);
    write(std::string(message_size, c));
  }

  std::uint64_t read_size = 0;

  // This should return data. 3 messages from the beginning, starting from '0'
  auto result = m_consumer->poll([&](auto new_data) {
    read_size = new_data.size();
    std::memcpy(m_consumer_buffer.data(), new_data.data(), new_data.size());
  });

  ASSERT_EQ(result, consumer::poll_event_type::new_data);
  ASSERT_EQ(read_size, 60);
  char expected = '0';

  std::span<std::byte> read_data{m_consumer_buffer.data(), read_size};
  for (auto read_msg : messages_range{read_data}) {
    std::string_view read_str{(const char *)read_msg.data(), read_msg.size()};
    EXPECT_EQ(read_str, std::string(message_size, expected));
    ++expected;
  }

  // This should return data. 2 messages from the beginning, starting from '3'
  result = m_consumer->poll([&](auto new_data) {
    read_size = new_data.size();
    std::memcpy(m_consumer_buffer.data(), new_data.data(), new_data.size());
  });

  ASSERT_EQ(result, consumer::poll_event_type::new_data);
  ASSERT_EQ(read_size, 40);
  expected = '3';

  read_data = std::span<std::byte>{m_consumer_buffer.data(), read_size};
  for (auto read_msg : messages_range{read_data}) {
    std::string_view read_str{(const char *)read_msg.data(), read_msg.size()};
    EXPECT_EQ(read_str, std::string(message_size, expected));
    ++expected;
  }
}

TEST_F(MessagingTest, MultipleMessagesOfExactlyQueueCapacity) {
  auto header = get_header();
  std::string msg(100, 'C');
  auto wrapped_message_size_with_header = msg.size() + sizeof(message_header);

  fill_leaving_space(wrapped_message_size_with_header - 1);

  for (auto c = 'a'; c < 'z'; ++c) {
    msg = std::string(header.m_capacity - sizeof(message_header), c);

    write(msg);

    std::uint64_t read_size = 0;

    auto result = m_consumer->poll([&](auto new_data) {
      read_size = new_data.size();
      std::memcpy(m_consumer_buffer.data(), new_data.data(), new_data.size());
    });

    ASSERT_EQ(result, consumer::poll_event_type::new_data) << c;
    ASSERT_EQ(read_size, msg.size() + sizeof(message_header));
    std::string_view read_msg{
        (const char *)(m_consumer_buffer.data() + sizeof(message_header)),
        read_size - sizeof(message_header)};
    EXPECT_EQ(read_msg, msg);
  }
}

TEST_F(MessagingTest, Random) {
  auto header = get_header();

  std::random_device rd;
  const auto seed = rd();
  std::mt19937 gen(seed);
  std::uniform_int_distribution<> distrib(0, header.m_capacity -
                                                 sizeof(message_header));
  std::uniform_int_distribution<char> char_distribution('a', 'z');

  std::cout << "[          ] " << seed << '\n';

  std::string msg(100, 'C');
  auto wrapped_message_size_with_header = msg.size() + sizeof(message_header);

  fill_leaving_space(wrapped_message_size_with_header - 1);

  for (int i = 0; i < 10000; ++i) {
    char c = char_distribution(gen);
    const auto size = distrib(gen);
    msg = std::string(size, c);

    write(msg);

    std::uint64_t read_size = 0;

    auto result = m_consumer->poll([&](auto new_data) {
      read_size = new_data.size();
      std::memcpy(m_consumer_buffer.data(), new_data.data(), new_data.size());
    });

    ASSERT_EQ(result, consumer::poll_event_type::new_data) << i;
    ASSERT_EQ(read_size, msg.size() + sizeof(message_header)) << i;
    std::string_view read_msg{
        (const char *)(m_consumer_buffer.data() + sizeof(message_header)),
        read_size - sizeof(message_header)};
    EXPECT_EQ(read_msg, msg) << i;
  }
}

TEST_F(MessagingTest, SyncLost) {

  resetup(100);

  // Fill up to 40
  fill_leaving_space(60);
  consume_everything();

  // Write 6 messages 20 bytes each (including message header). This should
  // overrun consumer and it should be able to read 3 messages from the
  // beginning, starting from '3'.
  const auto message_size = 20 - sizeof(message_header);

  for (char c = '0'; c < '6'; ++c) {
    auto msg = std::string(message_size, c);
    write(std::string(message_size, c));
  }

  std::uint64_t read_size = 0;

  // This should los sync.
  auto result = m_consumer->poll([&](auto new_data) {
    read_size = new_data.size();
    std::memcpy(m_consumer_buffer.data(), new_data.data(), new_data.size());
  });

  ASSERT_EQ(result, consumer::poll_event_type::lost_sync);

  // This should return data. 3 messages from the beginning, starting from '3'
  result = m_consumer->poll([&](auto new_data) {
    read_size = new_data.size();
    std::memcpy(m_consumer_buffer.data(), new_data.data(), new_data.size());
  });

  ASSERT_EQ(result, consumer::poll_event_type::new_data);
  ASSERT_EQ(read_size, 60);
  char expected = '3';

  std::span<std::byte> read_data{m_consumer_buffer.data(), read_size};
  for (auto read_msg : messages_range{read_data}) {
    std::string_view read_str{(const char *)read_msg.data(), read_msg.size()};
    EXPECT_EQ(read_str, std::string(message_size, expected));
    ++expected;
  }
}

TEST_F(MessagingTest, Interrupted) {

  std::string msg1 = "1111";
  std::string msg2 = "2222222";

  // Message 1
  m_producer->write(msg1.size(), [&](std::span<std::byte> buffer) {
    ASSERT_EQ(buffer.size(), msg1.size());
    std::memcpy(buffer.data(), msg1.data(), msg1.size());
  });

  std::uint64_t read_size = 0;

  auto result = m_consumer->poll([&](auto new_data) {
    read_size = new_data.size();
    std::memcpy(m_consumer_buffer.data(), new_data.data(), new_data.size());

    // Interrupt with message 2
    m_producer->write(msg2.size(), [&](std::span<std::byte> buffer) {
      ASSERT_EQ(buffer.size(), msg2.size());
      std::memcpy(buffer.data(), msg2.data(), msg2.size());
    });
  });

  ASSERT_EQ(result, consumer::poll_event_type::interrupted);

  result = m_consumer->poll([&](auto new_data) {
    read_size = new_data.size();
    std::memcpy(m_consumer_buffer.data(), new_data.data(), new_data.size());
  });

  ASSERT_EQ(result, consumer::poll_event_type::new_data);
  ASSERT_EQ(read_size, 2 * sizeof(message_header) + msg1.size() + msg2.size());

  auto read_msg = std::string_view{
      (const char *)(m_consumer_buffer.data() + sizeof(message_header)),
      msg1.size()};
  EXPECT_EQ(read_msg, msg1);

  read_msg =
      std::string_view{(const char *)(m_consumer_buffer.data() +
                                      2 * sizeof(message_header) + msg1.size()),
                       msg2.size()};
  EXPECT_EQ(read_msg, msg2);
}

TEST_F(MessagingTest, InterruptedSyncLost) {
  // TODO implement
}

TEST_F(MessagingTest, Issue1) {
  resetup(50);

  std::string msg1 = std::string(14, '1');
  std::string msg2 = std::string(4, '2');
  std::string msg3 = std::string(20, '3');

  write(msg1);

  std::string_view read_str;
  std::size_t read_size = 0;

  auto result = m_consumer->poll([&](auto new_data) {
    read_size = new_data.size();
    std::memcpy(m_consumer_buffer.data(), new_data.data(), new_data.size());
  });

  ASSERT_EQ(result, consumer::poll_event_type::new_data);

  std::span read_data(m_consumer_buffer.data(), read_size);

  std::vector<std::string_view> gotMessages;

  for (auto read_msg : messages_range{read_data}) {
    std::string_view read_str{(const char *)read_msg.data(), read_msg.size()};
    gotMessages.push_back(read_str);
  }

  EXPECT_EQ(gotMessages[0], msg1);

  write(msg2);
  write(msg3);

  result = m_consumer->poll([&](auto new_data) {
    read_size = new_data.size();
    std::memcpy(m_consumer_buffer.data(), new_data.data(), new_data.size());
    read_str = std::string_view{(const char *)m_consumer_buffer.data(),
                                new_data.size()};
  });

  ASSERT_EQ(result, consumer::poll_event_type::lost_sync);

  result = m_consumer->poll([&](auto new_data) {
    read_size = new_data.size();
    std::memcpy(m_consumer_buffer.data(), new_data.data(), new_data.size());
    read_str = std::string_view{(const char *)m_consumer_buffer.data(),
                                new_data.size()};
  });

  read_data = std::span{m_consumer_buffer.data(), read_size};

  ASSERT_EQ(result, consumer::poll_event_type::new_data);

  gotMessages.clear();

  for (auto read_msg : messages_range{read_data}) {
    std::string_view read_str{(const char *)read_msg.data(), read_msg.size()};
    gotMessages.push_back(read_str);
  }

  ASSERT_EQ(gotMessages.size(), 1);
  EXPECT_EQ(gotMessages[0], msg3);
}

TEST_F(MessagingTest, ConcurrentReadWriteNoSyncLost) {

  std::random_device rd;

  struct test_case {
    std::uint64_t m_q_size = 0;
    std::uint64_t m_max_msg_size = std::numeric_limits<std::uint64_t>::max();
  };

  const auto cases = {test_case{50}, test_case{100}, test_case{1024},
                      test_case{1337}, test_case{1024 * 1024, 1024}};
  const auto seeds = {3710639107u, rd()};

  for (auto tc : cases) {
    for (auto seed : seeds) {

      fmt::print("[          ] seed={}, q-size={}, max-msg-size={}\n", seed,
                 tc.m_q_size, tc.m_max_msg_size);

      resetup(tc.m_q_size);

      auto header = get_header();

      int N = 100000;

      std::uint64_t max_msg_size =
          tc.m_max_msg_size == test_case{}.m_max_msg_size
              ? header.m_capacity - sizeof(message_header)
              : tc.m_max_msg_size;

      std::mt19937 gen(seed);
      std::uniform_int_distribution<> size_distribution(0, max_msg_size);
      std::uniform_int_distribution<char> char_distribution('a', 'z');

      struct test_stage {
        std::string m_msg;
        std::uint64_t m_producer_end_before_this_stage = 0;
        std::uint64_t m_producer_end_after_this_stage = 0;
        int m_min_consumer_stage_i = 0;
      };

      std::vector<test_stage> test_stages;

      for (int i = 0; i < N; ++i) {

        test_stage stage;

        std::uint64_t next_msg_size = size_distribution(gen);
        char c = char_distribution(gen);
        stage.m_msg = std::string(next_msg_size, c);

        stage.m_producer_end_before_this_stage = get_header().m_end_offset;
        write(stage.m_msg);
        stage.m_producer_end_after_this_stage = get_header().m_end_offset;

        test_stages.push_back(stage);

        std::uint64_t current_stage_end = get_header().m_end_offset;

        bool found = false;

        for (int j = test_stages.size() - 1; j >= 0; --j) {
          auto diff = current_stage_end -
                      test_stages[j].m_producer_end_before_this_stage;

          if (diff <= header.m_capacity) {
            test_stages.back().m_min_consumer_stage_i = j;
          } else {
            if (test_stages.back().m_min_consumer_stage_i == 0) {
              test_stages.back().m_min_consumer_stage_i = j;
            }
            found = true;
            break;
          }
        }

        if (!found) {
          test_stages.back().m_min_consumer_stage_i = 0;
        }
      }

      resetup(tc.m_q_size);

      std::atomic_int consumer_i = 0;

      bool consumer_running = true;
      std::vector<std::string> messages;
      messages.resize(N);

      std::thread producer_thread{[&] {
        int stage_i = 0;

        while (stage_i < N) {

          if (!consumer_running) {
            return;
          }

          auto &stage = test_stages[stage_i];
          if (consumer_i.load() < stage.m_min_consumer_stage_i) {
            continue;
          }

          messages[stage_i] = stage.m_msg;
          write(stage.m_msg);
          ++stage_i;
        }
      }};

      std::thread consumer_thread{[&] {
        while (consumer_i.load() < N) {

          std::size_t read_size = 0;

          auto result = m_consumer->poll([&](auto new_data) {
            read_size = new_data.size();
            std::memcpy(m_consumer_buffer.data(), new_data.data(),
                        new_data.size());
          });

          ASSERT_NE(result, consumer::poll_event_type::lost_sync)
              << fmt::format("consumer_i={}", consumer_i.load());

          if (result != consumer::poll_event_type::new_data) {
            continue;
          }

          std::span read_data(m_consumer_buffer.data(), read_size);

          for (auto read_msg : messages_range{read_data}) {
            std::string_view read_str{(const char *)read_msg.data(),
                                      read_msg.size()};
            EXPECT_EQ(read_str, messages[consumer_i.load()])
                << consumer_i.load();
            consumer_i.store(consumer_i.load() + 1);
          }
        }

        consumer_running = false;
      }};

      producer_thread.join();
      consumer_thread.join();
    }
  }
}

TEST_F(MessagingTest, ConcurrentReadWrite) {

  std::random_device rd;

  struct test_case {
    std::uint64_t m_q_size = 0;
    std::uint64_t m_max_msg_size = std::numeric_limits<std::uint64_t>::max();
  };

  const auto cases = {test_case{50}, test_case{100}, test_case{1024},
                      test_case{1337}, test_case{1024 * 1024, 1024}};
  const auto seeds = {3710639107u, rd()};

  for (auto tc : cases) {
    for (auto seed : seeds) {

      fmt::print("[          ] seed={}, q-size={}, max-msg-size={}\n", seed,
                 tc.m_q_size, tc.m_max_msg_size);

      resetup(tc.m_q_size);

      auto header = get_header();

      int N = 100000;

      std::uint64_t max_msg_size =
          tc.m_max_msg_size == test_case{}.m_max_msg_size
              ? header.m_capacity - sizeof(message_header)
              : tc.m_max_msg_size;
      max_msg_size -= sizeof(std::uint64_t); // Checksum

      std::mt19937 gen(seed);
      std::uniform_int_distribution<> size_distribution(0, max_msg_size);
      std::uniform_int_distribution<char> char_distribution('a', 'z');

      bool producer_running = true;
      bool consumer_running = true;
      std::vector<std::string> messages;
      messages.resize(N);

      std::thread producer_thread{[&] {
        for (int i = 0; i < N; ++i) {
          if (!consumer_running) {
            return;
          }

          std::uint64_t next_msg_size = size_distribution(gen);
          char c = char_distribution(gen);
          auto msg = make_signed_message(std::string(next_msg_size, c));

          write(msg);
        }

        producer_running = false;
      }};

      std::thread consumer_thread{[&] {
        while (producer_running) {

          std::size_t read_size = 0;

          auto result = m_consumer->poll([&](auto new_data) {
            read_size = new_data.size();
            std::memcpy(m_consumer_buffer.data(), new_data.data(),
                        new_data.size());
          });

          if (result != consumer::poll_event_type::new_data) {
            continue;
          }

          std::span read_data(m_consumer_buffer.data(), read_size);

          for (auto read_msg : messages_range{read_data}) {
            std::string_view read_str{(const char *)read_msg.data(),
                                      read_msg.size()};

            EXPECT_TRUE(validate_signed_message(read_str)) << read_str;
          }
        }

        consumer_running = false;
      }};

      producer_thread.join();
      consumer_thread.join();
    }
  }
}

} // namespace cool_q::test
