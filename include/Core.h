#pragma once

#include <array>
#include <cstddef>

namespace kvaaas {
constexpr std::size_t KEY_SIZE_BITS = 128;                // TODO read in config
constexpr std::size_t KEY_SIZE_BYTES = KEY_SIZE_BITS / 8; // TODO read in config
constexpr std::size_t VALUE_SIZE_BYTES = 2 * (1 << 10);
using KeyType = std::array<std::byte, KEY_SIZE_BYTES>;
using ValueType = std::array<std::byte, VALUE_SIZE_BYTES>;
using ByteType = std::byte;
} // namespace kvaaas
