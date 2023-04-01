#include "cool_queues/cool_queues.hpp"

#include <gtest/gtest.h>

#include <array>

namespace cool_q::test {

TEST(BufferTest, Constructor) {
  std::array<std::byte, 1024> memory_buffer;
  buffer buf(memory_buffer);
  auto &header = buf.access_header();
  EXPECT_EQ(header.m_header_size, sizeof(header));
  EXPECT_EQ(header.m_writing_mark, 0);
  EXPECT_EQ(header.m_ready_mark, 0);
  EXPECT_EQ(header.m_version, 0);
  EXPECT_EQ(header.m_capacity, memory_buffer.size() - sizeof(header));

  header.m_writing_mark = 40;
  header.m_ready_mark = 20;
  header.m_version = 41;

  buffer buf2(memory_buffer, header);
  EXPECT_EQ(header.m_header_size, sizeof(header));
  EXPECT_EQ(header.m_writing_mark, 40);
  EXPECT_EQ(header.m_ready_mark, 20);
  EXPECT_EQ(header.m_version, 41);
  EXPECT_EQ(header.m_capacity, memory_buffer.size() - sizeof(header));
}

} // namespace cool_q::test