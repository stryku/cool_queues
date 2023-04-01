#pragma once

#include <cstdint>
#include <cstring>
#include <span>
#include <stdexcept>

namespace cool_q {

using message_size_t = std::uint32_t;
using buffer_version_t = std::uint32_t;

constexpr message_size_t k_end_of_messages = 0xffFFffFF;

struct buffer_header {
  std::uint64_t m_header_size = 0;
  // Starting from sizeof(header). It only grows. It should be calculated %
  // m_capacity.
  std::uint64_t m_end_offset = 0;
  // Starting from sizeof(header). It only grows. It should be calculated %
  // m_capacity.
  std::uint64_t m_footer_at_offset = 0;
  buffer_version_t m_version = 0;
  std::uint64_t m_capacity = 0; // Without header

  auto calc_end_offset() const {
    return m_end_offset % m_capacity;
  }

  auto calc_footer_at_offset() const {
    return m_footer_at_offset % m_capacity;
  }
};

// Space reserved at the end of memory_buffer for orchestration
struct buffer_footer {
  message_size_t m_end_of_messages = k_end_of_messages;
};

struct message_header {
  message_size_t m_size = 0;
};

class buffer {
public:
  // Constructor for initial queue creation.
  explicit buffer(std::span<std::byte> memory_buffer)
      : m_buffer{memory_buffer} {
    // Initialize header
    auto &header = access_header();
    header = {};
    header.m_header_size = sizeof(buffer_header);
    header.m_capacity =
        memory_buffer.size() - sizeof(buffer_header) - sizeof(buffer_footer);
  }

  // Constructor for queue with already initialized header.
  explicit buffer(std::span<std::byte> memory_buffer, const buffer_header &)
      : m_buffer{memory_buffer} {}

  buffer_header &access_header() const {
    return reinterpret_cast<buffer_header &>(*m_buffer.data());
  }

  std::span<std::byte> access_data() const {
    return m_buffer.subspan(sizeof(buffer_header));
  }

private:
  std::span<std::byte> m_buffer;
};

class producer {
public:
  explicit producer(std::span<std::byte> memory_buffer)
      : m_buffer{memory_buffer} {}

  template <typename WriteCallback>
  void write(message_size_t size, WriteCallback &&write_cb) {
    auto &header = m_buffer.access_header();
    auto data = m_buffer.access_data();

    if (size > data.size()) {
      // TODO: throw proper type
      throw std::runtime_error{"Q too small"};
    }

    const std::uint64_t available_capacity =
        header.m_capacity - header.calc_end_offset();

    if (available_capacity > sizeof(message_header) + size) {
      // Happy path, message fits in available space
      ++header.m_version;

      std::span<std::byte> write_buffer =
          data.subspan(header.calc_end_offset() + sizeof(message_header), size);
      write_cb(write_buffer);

      message_header &msg_header = reinterpret_cast<message_header &>(
          *(data.data() + header.calc_end_offset()));
      msg_header = message_header{.m_size = size};
      header.m_end_offset += sizeof(message_header) + size;
      ++header.m_version;
      return;
    }

    // Need to wrap and start from beginning
    ++header.m_version;

    // Write footer
    const buffer_footer footer{};
    std::memcpy(data.data() + header.calc_end_offset(), &footer,
                sizeof(footer));
    header.m_footer_at_offset = header.m_end_offset;

    std::span<std::byte> write_buffer =
        data.subspan(sizeof(message_header), size);
    write_cb(write_buffer);

    message_header &msg_header =
        reinterpret_cast<message_header &>(*data.data());
    msg_header = message_header{.m_size = size};
    const std::uint64_t offset_till_end =
        header.m_capacity - header.m_end_offset;
    header.m_end_offset = offset_till_end + sizeof(message_header) + size;
    ++header.m_version;
  }

private:
  buffer m_buffer;
};

class consumer {
public:
  enum class poll_event_type { no_new_data, new_data, lost_sync, interrupted };
  struct poll_result {
    poll_event_type m_event = poll_event_type::no_new_data;
    std::uint64_t m_read = 0;
  };

  explicit consumer(std::span<std::byte> memory_buffer)
      : m_buffer{memory_buffer,
                 reinterpret_cast<buffer_header &>(*memory_buffer.data())} {}

  poll_result poll(std::span<std::byte> result_buffer) {
    const auto header_before = m_buffer.access_header();
    if (header_before.m_version == m_version) {
      return {poll_event_type::no_new_data};
    }

    if (did_lost_sync(header_before)) {
      return {poll_event_type::lost_sync};
    }

    auto data = m_buffer.access_data();

    const auto end_offset = header_before.calc_end_offset();
    const auto read_offset = calc_read_offset(header_before.m_capacity);

    if (end_offset > read_offset) {
      // Happy path, no wrapping
      std::span<std::byte> new_data =
          data.subspan(read_offset, end_offset - read_offset);

      if (new_data.size() <= result_buffer.size()) {
        // Happy path, all the data fits in the result buffer.
        std::memcpy(result_buffer.data(), new_data.data(), new_data.size());

        const auto header_after = m_buffer.access_header();
        if (header_after.m_version != header_before.m_version) {
          // Producer wrote something in the meantime. Don't trust the data.
          return {poll_event_type::interrupted};
        } else {
          // All good
          m_read_offset = header_after.m_end_offset;
          return {poll_event_type::new_data, new_data.size()};
        }
      }

      // Only some new data fits in result buffer. Copy as much as possible.
      return read_message_by_message(result_buffer);
    }

    // Data got wrapped, read till the end

    const auto footer_offset = header_before.calc_footer_at_offset();

    std::span<std::byte> new_data =
        data.subspan(read_offset, footer_offset - read_offset);

    if (new_data.size() <= result_buffer.size()) {
      // Happy path, all the data fits in the result buffer.
      std::memcpy(result_buffer.data(), new_data.data(), new_data.size());

      const auto header_after = m_buffer.access_header();
      if (header_after.m_version != header_before.m_version) {
        // Producer wrote something in the meantime. Don't trust the data.
        return {poll_event_type::interrupted};
      }

      // All good. Wrap reading offset
      m_read_offset += header_after.m_capacity - read_offset;
      // Now, we're wrapped to the beginning of buffer. Poll again with the rest
      // of result_buffer

      const auto next_result_buffer = result_buffer.subspan(new_data.size());
      const poll_result result = poll(next_result_buffer);

      if (result.m_event == poll_event_type::new_data) {
        return {poll_event_type::new_data, new_data.size() + result.m_read};
      } else {
        // Something went wrong. Return data just from this read.
        return {poll_event_type::new_data, new_data.size()};
      }
    }

    // Only some new data fits in result buffer. Copy as much as possible.
    return read_message_by_message(result_buffer);
  }

private:
  poll_result read_message_by_message(std::span<std::byte> result_buffer) {

    bool copied_some = false;

    std::span<std::byte> current_buffer = result_buffer;

    while (true) {

      const auto header_before = m_buffer.access_header();

      if (header_before.m_end_offset == m_read_offset) {
        if (copied_some) {
          return {poll_event_type::new_data,
                  result_buffer.size() - current_buffer.size()};
        } else {
          return {poll_event_type::no_new_data};
        }
      }

      if (did_lost_sync(header_before)) {
        if (copied_some) {
          // Lost sync but copied some messages before that. Notify about new
          // messages and in the next poll we'll notify lost sync.
          return {poll_event_type::new_data,
                  result_buffer.size() - current_buffer.size()};
        } else {
          return {poll_event_type::lost_sync};
        }
      }

      // We have some data. Read one message
      auto data = m_buffer.access_data();

      const auto read_offset = calc_read_offset(header_before.m_capacity);

      const message_size_t msg_size =
          reinterpret_cast<message_header *>(data.data() + read_offset)->m_size;

      // TODO: check wrapping

      if (header_before.m_version != m_buffer.access_header().m_version) {
        // Interrupted, try again
        continue;
      }

      const message_size_t whole_msg_size = sizeof(message_header) + msg_size;

      if (whole_msg_size > current_buffer.size()) {
        // Message doesn't fit in the available buffer.
        if (copied_some) {
          return {poll_event_type::new_data,
                  result_buffer.size() - current_buffer.size()};
        } else {
          return {poll_event_type::lost_sync};
        }
      }

      auto msg_data = data.subspan(read_offset, whole_msg_size);
      std::memcpy(current_buffer.data(), msg_data.data(), msg_data.size());

      const auto header_after = m_buffer.access_header();
      if (header_before.m_version != header_after.m_version) {
        // Interrupted, try again
        continue;
      }

      // Successfully copied a message
      copied_some = true;
      current_buffer = current_buffer.subspan(msg_data.size());
      m_read_offset += msg_data.size();
    }
  }

  std::uint64_t calc_read_offset(std::uint64_t queue_capacity) const {
    return m_read_offset % queue_capacity;
  }

  bool did_lost_sync(const buffer_header &header) const {
    return header.m_end_offset - m_read_offset >= header.m_capacity;
  }

private:
  buffer m_buffer;
  // It only grows. It should be calculated % buffer_header.m_capacity.
  std::uint64_t m_read_offset = 0;
  buffer_version_t m_version = 0;
};

class messages_range {
public:
  explicit messages_range(std::span<std::byte> buffer) : m_buffer{buffer} {}

  struct iterator {
    explicit iterator(std::byte *data) : m_data{data} {}

    std::span<std::byte> operator*() const {
      message_size_t msg_size;
      std::memcpy(&msg_size, m_data, sizeof(message_size_t));
      return std::span{m_data + sizeof(message_size_t), msg_size};
    }

    iterator &operator++() {
      message_size_t msg_size;
      std::memcpy(&msg_size, m_data, sizeof(message_size_t));
      m_data += msg_size + sizeof(message_size_t);
      return *this;
    }

    iterator operator++(int) {
      auto copy = *this;
      message_size_t msg_size;
      std::memcpy(&msg_size, m_data, sizeof(message_size_t));
      m_data += msg_size + sizeof(message_size_t);
      return copy;
    }

    constexpr bool operator==(const iterator &) const = default;

    std::byte *m_data = nullptr;
  };

  iterator begin() const {
    return iterator{m_buffer.data()};
  }

  iterator end() const {
    return iterator{m_buffer.data() + m_buffer.size()};
  }

private:
  std::span<std::byte> m_buffer;
};

} // namespace cool_q
