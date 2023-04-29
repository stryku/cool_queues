#pragma once

#include <fmt/format.h>

#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <span>
#include <stdexcept>

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
  buffer_version_t m_version = 0;
  std::uint64_t m_capacity = 0; // Without header

  auto calc_end_offset() const {
    return m_end_offset % m_capacity;
  }
};

// Space reserved at the end of memory_buffer for orchestration
struct buffer_footer {
  message_size_t m_end_of_messages = k_end_of_messages;
};

struct message_header {
  message_size_t m_size = 0;
  std::uint32_t m_seq = 0;
};
} // namespace cool_q

template <> struct fmt::formatter<cool_q::buffer_header> {
  constexpr auto parse(auto &ctx) const {
    return ctx.begin();
  }

  constexpr auto format(const cool_q::buffer_header &header, auto &ctx) {
    return format_to(ctx.out(),
                     "size={}, end-offset={}, version={}, capacity={}",
                     header.m_header_size, header.m_end_offset,
                     header.m_version, header.m_capacity);
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

  // Returns buffer including space for footer.
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

  void write(message_size_t size, auto &&write_cb) {

    auto &header = m_buffer.access_header();
    ++header.m_end_offset;
    const auto true_end_offset = header.m_end_offset / 2;
    std::atomic_thread_fence(std::memory_order_release);

    auto data = m_buffer.access_data();

    COOL_Q_PRODUCER_LOG(
        fmt::format("Writing msg-size={}, header=({})", size, header));

    if (sizeof(message_header) + size > header.m_capacity) {
      // TODO: throw proper type
      throw std::runtime_error{"Q too small"};
    }

    const std::uint64_t available_capacity = [&] {
      const auto calced_end_offset = true_end_offset % header.m_capacity;
      if (true_end_offset != 0) {
        if (calced_end_offset == 0) {
          return std::uint64_t{0};
        }
        return header.m_capacity - calced_end_offset;
      }

      return header.m_capacity;
    }();

    if (available_capacity >= sizeof(message_header) + size) {
      // Happy path, message fits in available space
      ++header.m_version;

      std::span<std::byte> write_buffer = data.subspan(
          true_end_offset % header.m_capacity + sizeof(message_header), size);
      write_cb(write_buffer);

      message_header &msg_header = reinterpret_cast<message_header &>(
          *(data.data() + true_end_offset % header.m_capacity));
      msg_header = message_header{.m_size = size, .m_seq = ++m_seq};
      header.m_end_offset += (sizeof(message_header) + size) * 2;
      --header.m_end_offset;
      std::atomic_thread_fence(std::memory_order_release);
      ++header.m_version;
      return;
    }

    // Need to wrap and start from beginning
    ++header.m_version;

    // Write footer
    const buffer_footer footer{};
    if (true_end_offset % header.m_capacity == 0) {
      std::memcpy(data.data() + header.m_capacity, &footer, sizeof(footer));
    } else {
      std::memcpy(data.data() + true_end_offset % header.m_capacity, &footer,
                  sizeof(footer));
    }

    std::span<std::byte> write_buffer =
        data.subspan(sizeof(message_header), size);
    assert(write_buffer.data() + size < data.data() + data.size());
    write_cb(write_buffer);
    std::atomic_thread_fence(std::memory_order_release);

    // TODO memcpy
    message_header &msg_header =
        reinterpret_cast<message_header &>(*data.data());
    msg_header = message_header{.m_size = size, .m_seq = ++m_seq};

    const std::uint64_t offset_till_end = available_capacity;

    // Basically point on the beginning of the Q.

    header.m_end_offset +=
        (offset_till_end + sizeof(message_header) + size) * 2;
    std::atomic_thread_fence(std::memory_order_release);

    --header.m_end_offset;

    ++header.m_version;
  }

private:
  buffer m_buffer;
  std::uint32_t m_seq = 0;
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

  auto read_end_offset() {
    std::atomic_thread_fence(std::memory_order_acquire);
    return m_buffer.access_header().m_end_offset;
  }

  poll_event_type poll(auto poll_cb) {
    const auto end_offset_before = read_end_offset();
    const auto true_end_offset = end_offset_before / 2;

    if (end_offset_before & 1) {
      // Producer is busy. Don't read now.
      return poll_event_type::no_new_data;
    }

    // const auto header_before = m_buffer.access_header();
    const auto capacity = m_buffer.access_header().m_capacity;

    COOL_Q_CONSUMER_LOG(
        fmt::format("polling read-offset={}, wrap={}, header={}", m_read_offset,
                    m_queue_wrap_offset, m_buffer.access_header()));

    // Check sync lost
    {
      if (true_end_offset - m_read_offset > capacity) {
        // Overrun. Need to go to begin.
        if (true_end_offset % capacity == 0) {
          m_queue_wrap_offset = (true_end_offset / capacity - 1) * capacity;
        } else {
          m_queue_wrap_offset = (true_end_offset / capacity) * capacity;
        }
        m_read_offset = m_queue_wrap_offset;
        const auto msg_header = read_current_message_header();
        if (msg_header.m_seq != m_seq + 1) {
          // Sync lost
          return poll_event_type::lost_sync;
        }
      }

      // All good
    }

    auto read_start = m_read_offset;
    auto current_read = m_read_offset;
    auto last_seq_seen = m_seq;

    while (true) {

      if (read_end_offset() != end_offset_before) {
        return poll_event_type::interrupted;
      }

      if (current_read == true_end_offset) {
        // No more data to read.

        if (current_read == read_start) {
          // TODO can ever happen?
          return poll_event_type::no_new_data;
        }

        // Did read some
        const auto data = m_buffer.access_data();
        std::span<std::byte> new_data{data.data() +
                                          (read_start - m_queue_wrap_offset),
                                      current_read - read_start};
        poll_cb(new_data);
        if (read_end_offset() != end_offset_before) {
          return poll_event_type::interrupted;
        } else {
          m_seq = last_seq_seen;
          m_read_offset = current_read;
          return poll_event_type::new_data;
        }
      }

      const message_size_t msg_size = read_message_size_at(current_read);
      if (read_end_offset() != end_offset_before) {
        return poll_event_type::interrupted;
      }

      if (msg_size == k_end_of_messages) {
        // Went all the way to the end of messages. There may be new messages
        // wrapped. They will be read in the next poll calls.

        // TODO Need to wrap
        if (current_read == read_start) {
          // Didn't read nothing.
          // TODO do read
          m_queue_wrap_offset += capacity;
          m_read_offset = m_queue_wrap_offset;
          read_start = m_read_offset;
          current_read = m_read_offset;
          continue;
        }

        const auto data = m_buffer.access_data();
        std::span<std::byte> new_data{data.data() +
                                          (read_start - m_queue_wrap_offset),
                                      current_read - read_start};
        poll_cb(new_data);
        if (read_end_offset() != end_offset_before) {
          return poll_event_type::interrupted;
        } else {
          m_queue_wrap_offset += capacity;
          m_read_offset = m_queue_wrap_offset;
          m_seq = last_seq_seen;
          return poll_event_type::new_data;
        }
      }

      // Got new message. Acknowledge it and iterate again.
      const auto msg_header = read_message_header_at(current_read);
      if (read_end_offset() != end_offset_before) {
        return poll_event_type::interrupted;
      }
      current_read += msg_header.m_size + sizeof(message_header);
      last_seq_seen = msg_header.m_seq;
    }
  }

private:
  message_header read_current_message_header() {
    return read_message_header_at(m_read_offset);
  }

  message_size_t read_message_size_at(std::uint64_t offset) {
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

private:
  buffer m_buffer;
  // It only grows. It should be calculated % buffer_header.m_capacity.
  std::uint64_t m_read_offset = 0;
  std::uint64_t m_queue_wrap_offset = 0;
  std::uint32_t m_seq = 0;
};

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
