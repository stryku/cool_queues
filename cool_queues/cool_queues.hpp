#pragma once

#include <fmt/format.h>

#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <stdexcept>
#include <vector>

#ifndef COOL_Q_PRODUCER_LOG
#define COOL_Q_PRODUCER_LOG(x) ((void)0)
#endif
#ifndef COOL_Q_CONSUMER_LOG
#define COOL_Q_CONSUMER_LOG(x) ((void)0)
#endif

namespace cool_q {

using message_size_t = std::uint32_t;
using buffer_version_t = std::uint32_t;

constexpr message_size_t k_end_of_messages = 0xffFFffFF;

struct buffer_header {
  std::uint64_t m_header_size = 0;
  // Offset of the end of data known to producer. The end of the last written
  // message. Starting from sizeof(header). It only grows. It should be
  // calculated % m_capacity.
  std::uint64_t m_end_offset = 0;
  // Where footer is located.
  // If 0, then it should be ignored and read msg by msg.
  // If not zero, footer is valid and can read up to footer, at once.
  std::uint64_t m_footer_at_offset = 0;
  std::uint64_t m_capacity = 0; // Without header
  std::uint32_t m_last_seq = 0;

  auto calc_end_offset() const {
    return m_end_offset % m_capacity;
  }
};

// Space reserved at the end of memory_buffer for orchestration
struct buffer_footer {
  message_size_t m_end_of_messages = k_end_of_messages;
  std::uint32_t m_last_msg_seq = 0;
};

struct message_header {
  message_size_t m_size = 0;
  std::uint32_t m_seq = 0;
};

enum class poll_event_type { no_new_data, new_data, lost_sync, interrupted };

class messages_range {
public:
  explicit messages_range(std::span<std::byte> buffer) : m_buffer{buffer} {}

  struct iterator {
    explicit iterator(std::byte *data) : m_data{data} {}

    std::span<std::byte> operator*() const {
      message_header msg_header;
      std::memcpy(&msg_header, m_data, sizeof(message_header));
      return std::span{m_data + sizeof(message_header), msg_header.m_size};
    }

    iterator &operator++() {
      message_header msg_header;
      std::memcpy(&msg_header, m_data, sizeof(message_header));
      m_data += msg_header.m_size + sizeof(message_header);
      return *this;
    }

    iterator operator++(int) {
      auto copy = *this;
      message_header msg_header;
      std::memcpy(&msg_header, m_data, sizeof(message_header));
      m_data += msg_header.m_size + sizeof(message_header);
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

template <> struct fmt::formatter<cool_q::buffer_header> {
  constexpr auto parse(auto &ctx) const {
    return ctx.begin();
  }

  constexpr auto format(const cool_q::buffer_header &header, auto &ctx) {
    return format_to(ctx.out(), "size={}, end-offset={}, capacity={}",
                     header.m_header_size, header.m_end_offset,
                     header.m_capacity);
  }
};

namespace cool_q {

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

  buffer_footer &access_footer() const {
    return reinterpret_cast<buffer_footer &>(*m_buffer.data());
  }

  // Returns buffer including space for footer.
  std::span<std::byte> access_data() const {
    return m_buffer.subspan(sizeof(buffer_header));
  }

private:
  std::span<std::byte> m_buffer;
};

template <std::uint64_t Capacity = 0> class producer {
public:
  explicit producer(std::span<std::byte> memory_buffer)
      : m_buffer{memory_buffer} {

    if constexpr (Capacity != 0) {
      if (memory_buffer.size() < required_buffer()) {
        throw std::runtime_error{
            fmt::format("Memory buffer={} too small for required capacity={} + "
                        "header={} + footer={}",
                        memory_buffer.size(), Capacity, sizeof(buffer_header),
                        sizeof(buffer_footer))};
      }

      m_buffer.access_header().m_capacity = Capacity;
    }
  }

  void write(message_size_t size, auto &&write_cb) {
    auto &header = m_buffer.access_header();
    ++header.m_end_offset;
    std::atomic_thread_fence(std::memory_order_release);
    const auto true_end_offset = header.m_end_offset >> 1;

    auto data = m_buffer.access_data();

    COOL_Q_PRODUCER_LOG(
        fmt::format("Writing msg-size={}, header=({})", size, header));

    if (sizeof(message_header) + size > get_capacity()) {
      throw std::runtime_error{"Q too small"};
    }

    const std::uint64_t available_capacity = calc_available_capacity();

    if (available_capacity >= sizeof(message_header) + size) {
      // Happy path, message fits in available space
      std::span<std::byte> write_buffer = data.subspan(
          true_end_offset % get_capacity() + sizeof(message_header), size);
      write_cb(write_buffer);

      message_header &msg_header = reinterpret_cast<message_header &>(
          *(data.data() + true_end_offset % get_capacity()));
      msg_header = message_header{.m_size = size, .m_seq = ++m_seq};
      header.m_end_offset += (sizeof(message_header) + size) << 1;

    } else {

      // Need to wrap and start from beginning

      // Write footer
      const buffer_footer footer{.m_last_msg_seq = m_seq};
      if (true_end_offset % get_capacity() == 0) {
        std::memcpy(data.data() + get_capacity(), &footer, sizeof(footer));
        header.m_footer_at_offset = true_end_offset;
      } else {
        std::memcpy(data.data() + true_end_offset % get_capacity(), &footer,
                    sizeof(footer));
        header.m_footer_at_offset = true_end_offset;
      }

      // Let user write content.
      std::span<std::byte> write_buffer =
          data.subspan(sizeof(message_header), size);
      assert(write_buffer.data() + size < data.data() + data.size());
      write_cb(write_buffer);

      // Write header.
      const message_header msg_header = {.m_size = size, .m_seq = ++m_seq};
      std::memcpy(data.data(), &msg_header, sizeof(message_header));

      // Point on the beginning of the Q.
      const std::uint64_t offset_till_end = available_capacity;
      header.m_end_offset +=
          (offset_till_end + sizeof(message_header) + size) * 2;
    }

    if ((header.m_end_offset >> 1) >
        header.m_footer_at_offset + get_capacity()) {
      // Footer got overrun, disable it.
      header.m_footer_at_offset = 0;
    }

    header.m_last_seq = m_seq;
    // Has to be as the last write. This is important..
    --header.m_end_offset;
    std::atomic_thread_fence(std::memory_order_release);
  }

  static constexpr std::uint64_t required_buffer() {
    if constexpr (Capacity == 0) {
      // Doesn't make much sense to use this method in this case...
      return 0;
    } else {
      return Capacity + sizeof(buffer_header) + sizeof(buffer_footer);
    }
  }

private:
  std::uint64_t calc_available_capacity() const {
    const auto true_end_offset = m_buffer.access_header().m_end_offset >> 1;

    const auto calced_end_offset = true_end_offset % get_capacity();

    if (true_end_offset != 0) {
      if (calced_end_offset == 0) {
        return std::uint64_t{0};
      }
      return get_capacity() - calced_end_offset;
    }

    return get_capacity();
  }

  constexpr std::uint64_t get_capacity() const {
    if constexpr (Capacity == 0) {
      return m_buffer.access_header().m_capacity;
    } else {
      return Capacity;
    }
  }

  buffer m_buffer;
  std::uint32_t m_seq = 0;
};

template <std::uint64_t Capacity = 0> class consumer {
public:
  explicit consumer(std::span<std::byte> memory_buffer)
      : m_buffer{memory_buffer,
                 reinterpret_cast<buffer_header &>(*memory_buffer.data())} {}

  poll_event_type poll(auto poll_cb) {
    const auto end_offset_before = access_header().m_end_offset;
    if (end_offset_before & 1) {
      // Producer is busy. Don't read now.
      return poll_event_type::no_new_data;
    }

    COOL_Q_CONSUMER_LOG(
        fmt::format("polling read-offset={}, wrap={}, header={}", m_read_offset,
                    m_queue_wrap_offset, access_header()));

    if (const auto result = check_sync_lost(end_offset_before);
        result.has_value()) {
      return *result;
    }

    if (const auto result = try_read_till_footer(end_offset_before, poll_cb);
        result.has_value()) {
      return *result;
    }

    if (const auto result =
            try_read_till_producer_end(end_offset_before, poll_cb);
        result.has_value()) {
      return *result;
    }

    return read_message_by_message(end_offset_before, poll_cb);
  }

private:
  std::optional<poll_event_type>
  check_sync_lost(std::uint64_t end_offset_before) {
    const std::uint64_t true_end_offset = end_offset_before >> 1;

    if (true_end_offset - m_read_offset > get_capacity()) {
      // Overrun. Need to go to begin.

      if (true_end_offset % get_capacity() == 0) {
        m_queue_wrap_offset =
            (true_end_offset / get_capacity() - 1) * get_capacity();
      } else {
        m_queue_wrap_offset =
            (true_end_offset / get_capacity()) * get_capacity();
      }

      m_read_offset = m_queue_wrap_offset;

      const auto msg_header = read_message_header_at(m_read_offset);
      if (msg_header.m_seq != m_seq + 1) {
        return poll_event_type::lost_sync;
      }
    }

    return std::nullopt;
  }

  std::optional<poll_event_type>
  try_read_till_footer(std::uint64_t end_offset_before, const auto &poll_cb) {

    const auto footer_at_offset = access_header().m_footer_at_offset;
    if (access_header().m_end_offset != end_offset_before) {
      return poll_event_type::interrupted;
    }

    const std::uint64_t true_end_offset = end_offset_before >> 1;

    if (footer_at_offset != 0 &&
        m_read_offset > true_end_offset - get_capacity() &&
        m_read_offset < footer_at_offset) {

      // Can read [read_start, footer)
      const auto data = m_buffer.access_data();
      const auto read_size = footer_at_offset - m_read_offset;
      std::span<std::byte> new_data{
          data.data() + (m_read_offset - m_queue_wrap_offset), read_size};

      poll_cb(new_data);

      if (access_header().m_end_offset != end_offset_before) {
        return poll_event_type::interrupted;
      } else {
        // We read up until footer, meaning we need to wrap. Let's do it right
        // away.
        m_queue_wrap_offset += get_capacity();
        m_read_offset = m_queue_wrap_offset;
        m_seq = m_buffer.access_footer().m_last_msg_seq;
        return poll_event_type::new_data;
      }
    }

    return std::nullopt;
  }

  std::optional<poll_event_type>
  try_read_till_producer_end(std::uint64_t end_offset_before,
                             const auto &poll_cb) {

    const std::uint64_t true_end_offset = end_offset_before >> 1;

    if (true_end_offset != m_read_offset &&
        true_end_offset - m_read_offset < get_capacity()) {

      const message_size_t msg_size = read_message_size_at(m_read_offset);
      if (access_header().m_end_offset != end_offset_before) {
        return poll_event_type::interrupted;
      }

      if (msg_size == k_end_of_messages) {
        // Went all the way to the end of messages. There may be new messages
        // wrapped. They will be read in the next poll calls.

        // Need to wrap
        m_queue_wrap_offset += get_capacity();
        m_read_offset = m_queue_wrap_offset;
      }

      // Can read [read_start, true_end_offset)
      const auto data = m_buffer.access_data();
      const auto read_size = true_end_offset - m_read_offset;
      std::span<std::byte> new_data{
          data.data() + (m_read_offset - m_queue_wrap_offset), read_size};

      poll_cb(new_data);

      std::atomic_thread_fence(std::memory_order_acquire);
      const auto seq = m_buffer.access_header().m_last_seq;

      if (access_header().m_end_offset != end_offset_before) {
        return poll_event_type::interrupted;
      } else {
        // We read up until footer, meaning we need to wrap. Let's do it right
        // away.
        m_read_offset = true_end_offset;
        m_seq = seq;
        return poll_event_type::new_data;
      }
    }

    return std::nullopt;
  }

  poll_event_type read_message_by_message(std::uint64_t end_offset_before,
                                          const auto &poll_cb) {

    const std::uint64_t true_end_offset = end_offset_before >> 1;

    auto read_start = m_read_offset;
    auto current_read = m_read_offset;
    auto last_seq_seen = m_seq;

    while (true) {
      if (current_read == true_end_offset) {
        // No more data to read.

        if (current_read == read_start) {
          // Read nothing.
          return poll_event_type::no_new_data;
        }

        // Did read some.
        const auto data = m_buffer.access_data();
        std::span<std::byte> new_data{data.data() +
                                          (read_start - m_queue_wrap_offset),
                                      current_read - read_start};
        poll_cb(new_data);
        if (access_header().m_end_offset != end_offset_before) {
          return poll_event_type::interrupted;
        } else {
          m_seq = last_seq_seen;
          m_read_offset = current_read;
          return poll_event_type::new_data;
        }
      }

      // There's still something to read.

      const auto msg_size = read_message_size_at(current_read);

      if (access_header().m_end_offset != end_offset_before) {
        return poll_event_type::interrupted;
      }

      if (msg_size == k_end_of_messages) {
        // Went all the way to the end of messages. There may be new messages
        // wrapped. They will be read in the next poll calls.

        if (current_read == read_start) {
          // Haven't read nothing yet. Wrap and try reading again.
          m_queue_wrap_offset += get_capacity();
          m_read_offset = m_queue_wrap_offset;
          read_start = m_read_offset;
          current_read = m_read_offset;
          continue;
        }

        // We read till end of messages. Notify user.

        const auto data = m_buffer.access_data();
        std::span<std::byte> new_data{data.data() +
                                          (read_start - m_queue_wrap_offset),
                                      current_read - read_start};
        poll_cb(new_data);
        if (access_header().m_end_offset != end_offset_before) {
          return poll_event_type::interrupted;
        } else {
          m_queue_wrap_offset += get_capacity();
          m_read_offset = m_queue_wrap_offset;
          m_seq = last_seq_seen;

          return poll_event_type::new_data;
        }
      }

      // Got new message. Acknowledge it and iterate again.

      const auto msg_header = read_message_header_at(current_read);
      if (access_header().m_end_offset != end_offset_before) {
        return poll_event_type::interrupted;
      }

      current_read += msg_header.m_size + sizeof(message_header);
      last_seq_seen = msg_header.m_seq;
    }
  }

  const buffer_header &access_header() const {
    std::atomic_thread_fence(std::memory_order_acquire);
    return m_buffer.access_header();
  }

  message_size_t read_message_size_at(std::uint64_t offset) const {
    message_size_t size;
    const auto read_ptr =
        m_buffer.access_data().data() + (offset - m_queue_wrap_offset);
    std::memcpy(&size, read_ptr, sizeof(message_size_t));
    return size;
  }

  message_header read_message_header_at(std::uint64_t offset) {
    message_header hdr;
    const std::byte *read_ptr =
        m_buffer.access_data().data() + (offset - m_queue_wrap_offset);
    std::memcpy(&hdr, read_ptr, sizeof(message_header));
    return hdr;
  }

  constexpr std::uint64_t get_capacity() const {
    if constexpr (Capacity == 0) {
      return m_buffer.access_header().m_capacity;
    } else {
      return Capacity;
    }
  }

private:
  buffer m_buffer;
  // It only grows. It should be calculated - m_queue_wrap_offset.
  std::uint64_t m_read_offset = 0;
  // It only grows.
  std::uint64_t m_queue_wrap_offset = 0;
  std::uint32_t m_seq = 0;
};

} // namespace cool_q
