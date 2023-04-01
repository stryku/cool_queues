#include "cool_queues/cool_queues.hpp"

#include <gtest/gtest.h>

#include <array>
#include <string>
#include <string_view>

namespace cool_q::test {

using namespace std::literals;

namespace {
constexpr std::array k_messages{"111111"sv,
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
    m_memory_buffer = {};
    m_consumer_buffer = {};
    m_producer = std::make_unique<producer>(m_memory_buffer);
    m_consumer = std::make_unique<consumer>(m_memory_buffer);
  }

  void TearDown() override {
    m_producer.reset();
    m_consumer.reset();
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
    m_consumer->poll(m_consumer_buffer);
  }

  void write(std::string_view msg) {
    m_producer->write(msg.size(), [&](std::span<std::byte> buffer) {
      ASSERT_EQ(buffer.size(), msg.size());
      std::memcpy(buffer.data(), msg.data(), msg.size());
    });
  }

  std::array<std::byte, 1024> m_memory_buffer;
  std::array<std::byte, 1024> m_consumer_buffer;
  std::unique_ptr<producer> m_producer;
  std::unique_ptr<consumer> m_consumer;
};

TEST(BufferTest, Constructor) {
  std::array<std::byte, 1024> memory_buffer;

  std::fill(memory_buffer.begin(), memory_buffer.end(), std::byte{0xff});

  buffer buf(memory_buffer);
  auto &header = buf.access_header();
  EXPECT_EQ(header.m_header_size, sizeof(header));
  // EXPECT_EQ(header.m_last_message_offset, 0);
  EXPECT_EQ(header.m_end_offset, 0);
  EXPECT_EQ(header.m_footer_at_offset, 0);
  EXPECT_EQ(header.m_version, 0);
  EXPECT_EQ(header.m_capacity, memory_buffer.size() - sizeof(buffer_header) -
                                   sizeof(buffer_footer));

  // header.m_last_message_offset = 30;
  header.m_end_offset = 40;
  header.m_footer_at_offset = 20;
  header.m_version = 41;

  buffer buf2(memory_buffer, header);
  EXPECT_EQ(header.m_header_size, sizeof(header));
  // EXPECT_EQ(header.m_last_message_offset, 30);
  EXPECT_EQ(header.m_end_offset, 40);
  EXPECT_EQ(header.m_footer_at_offset, 20);
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
  // EXPECT_EQ(header.m_last_message_offset, 0);
  EXPECT_EQ(header.m_end_offset, 0);
  EXPECT_EQ(header.m_footer_at_offset, 0);
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
  // header.m_last_message_offset = 30;
  header.m_end_offset = 40;
  header.m_footer_at_offset = 20;
  header.m_version = 41;

  consumer consumer(memory_buffer);
  // Ensure it doesn't override header
  EXPECT_EQ(header.m_header_size, sizeof(header));
  // EXPECT_EQ(header.m_last_message_offset, 30);
  EXPECT_EQ(header.m_end_offset, 40);
  EXPECT_EQ(header.m_footer_at_offset, 20);
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

  auto result = m_consumer->poll(m_consumer_buffer);
  ASSERT_EQ(result.m_event, consumer::poll_event_type::new_data);
  ASSERT_EQ(result.m_read, msg1.size() + sizeof(message_header));
  std::string_view read_msg{
      (const char *)(m_consumer_buffer.data() + sizeof(message_header)),
      result.m_read - sizeof(message_header)};
  EXPECT_EQ(read_msg, msg1);

  // Message 2
  m_producer->write(msg2.size(), [&](std::span<std::byte> buffer) {
    ASSERT_EQ(buffer.size(), msg2.size());
    std::memcpy(buffer.data(), msg2.data(), msg2.size());
  });

  result = m_consumer->poll(m_consumer_buffer);
  ASSERT_EQ(result.m_event, consumer::poll_event_type::new_data);
  ASSERT_EQ(result.m_read, msg2.size() + sizeof(message_header));
  read_msg = std::string_view{
      (const char *)(m_consumer_buffer.data() + sizeof(message_header)),
      result.m_read - sizeof(message_header)};
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

  result = m_consumer->poll(m_consumer_buffer);
  ASSERT_EQ(result.m_event, consumer::poll_event_type::new_data);
  ASSERT_EQ(result.m_read,
            2 * sizeof(message_header) + msg1.size() + msg2.size());

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

  auto result = m_consumer->poll(m_consumer_buffer);
  ASSERT_EQ(result.m_event, consumer::poll_event_type::new_data);
  std::span<std::byte> read_data{m_consumer_buffer.data(), result.m_read};
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
    auto result = consumer.m_consumer.poll(consumer.m_buffer);
    ASSERT_EQ(result.m_event, consumer::poll_event_type::new_data);
    std::span<std::byte> read_data{consumer.m_buffer.data(), result.m_read};
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
      auto result = consumer.m_consumer.poll(consumer.m_buffer);
      ASSERT_EQ(result.m_event, consumer::poll_event_type::new_data);
      std::span<std::byte> read_data{consumer.m_buffer.data(), result.m_read};
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

  auto result = m_consumer->poll(m_consumer_buffer);
  ASSERT_EQ(result.m_event, consumer::poll_event_type::new_data);
  ASSERT_EQ(result.m_read, msg.size() + sizeof(message_header));
  std::string_view read_msg{
      (const char *)(m_consumer_buffer.data() + sizeof(message_header)),
      result.m_read - sizeof(message_header)};
  EXPECT_EQ(read_msg, msg);
}

TEST_F(MessagingTest, MessageOneByteTooBig) {
  std::string msg(100, 'C');
  auto wrapped_message_size_with_header = msg.size() + sizeof(message_header);

  fill_leaving_space(wrapped_message_size_with_header - 1);

  write(msg);

  auto result = m_consumer->poll(m_consumer_buffer);
  ASSERT_EQ(result.m_event, consumer::poll_event_type::new_data);
  ASSERT_EQ(result.m_read, msg.size() + sizeof(message_header));
  std::string_view read_msg{
      (const char *)(m_consumer_buffer.data() + sizeof(message_header)),
      result.m_read - sizeof(message_header)};
  EXPECT_EQ(read_msg, msg);
}

// TEST_F(MessagingTest, MessageOfExactlyQueueCapacity) {
//   auto header = get_header();
//   std::string msg(100, 'C');
//   auto wrapped_message_size_with_header = msg.size() +
//   sizeof(message_header);

//   fill_leaving_space(wrapped_message_size_with_header - 1);

//   msg = std::string(header.m_capacity - sizeof(message_header), 'c');

//   write(msg);

//   auto result = m_consumer->poll(m_consumer_buffer);
//   ASSERT_EQ(result.m_event, consumer::poll_event_type::new_data);
//   ASSERT_EQ(result.m_read, msg.size() + sizeof(message_header));
//   std::string_view read_msg{
//       (const char *)(m_consumer_buffer.data() + sizeof(message_header)),
//       result.m_read - sizeof(message_header)};
//   EXPECT_EQ(read_msg, msg);
// }

} // namespace cool_q::test