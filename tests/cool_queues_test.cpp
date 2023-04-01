#include "cool_queues/cool_queues.hpp"

#include <gtest/gtest.h>

#include <array>

namespace cool_q::test {

TEST(BufferTest, Constructor) {
  std::array<std::byte, 1024> memory_buffer;
  buffer buf(memory_buffer);
  auto &header = buf.access_header();
  EXPECT_EQ(header.m_header_size, sizeof(header));
  EXPECT_EQ(header.m_end_offset, 0);
  EXPECT_EQ(header.m_footer_at_offset, 0);
  EXPECT_EQ(header.m_version, 0);
  EXPECT_EQ(header.m_capacity, memory_buffer.size() - sizeof(buffer_header) -
                                   sizeof(buffer_footer));

  header.m_end_offset = 40;
  header.m_footer_at_offset = 20;
  header.m_version = 41;

  buffer buf2(memory_buffer, header);
  EXPECT_EQ(header.m_header_size, sizeof(header));
  EXPECT_EQ(header.m_end_offset, 40);
  EXPECT_EQ(header.m_footer_at_offset, 20);
  EXPECT_EQ(header.m_version, 41);
  EXPECT_EQ(header.m_capacity, memory_buffer.size() - sizeof(buffer_header) -
                                   sizeof(buffer_footer));
}

TEST(ProducerTest, Constructor) {
  std::array<std::byte, 1024> memory_buffer;
  producer producer{memory_buffer};

  buffer buffer(memory_buffer, buffer_header{});
  auto &header = buffer.access_header();
  EXPECT_EQ(header.m_header_size, sizeof(header));
  EXPECT_EQ(header.m_end_offset, 0);
  EXPECT_EQ(header.m_footer_at_offset, 0);
  EXPECT_EQ(header.m_version, 0);
  EXPECT_EQ(header.m_capacity, memory_buffer.size() - sizeof(buffer_header) -
                                   sizeof(buffer_footer));
}

TEST(ConsumerTest, Constructor) {
  std::array<std::byte, 1024> memory_buffer;
  producer producer{memory_buffer};

  buffer buffer(memory_buffer, buffer_header{});
  auto &header = buffer.access_header();
  header.m_end_offset = 40;
  header.m_footer_at_offset = 20;
  header.m_version = 41;

  consumer consumer(memory_buffer);
  // Ensure it doesn't override header
  EXPECT_EQ(header.m_header_size, sizeof(header));
  EXPECT_EQ(header.m_end_offset, 40);
  EXPECT_EQ(header.m_footer_at_offset, 20);
  EXPECT_EQ(header.m_version, 41);
  EXPECT_EQ(header.m_capacity, memory_buffer.size() - sizeof(buffer_header) -
                                   sizeof(buffer_footer));
}

} // namespace cool_q::test