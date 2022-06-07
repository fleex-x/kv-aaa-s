#pragma once

#include "xxhash.h"
#include <array>
#include <cstddef>
#include <functional>
#include <string>
#include <vector>

namespace kvaaas {
constexpr std::size_t KEY_SIZE_BITS = 128;                // TODO read in config
constexpr std::size_t KEY_SIZE_BYTES = KEY_SIZE_BITS / 8; // TODO read in config
constexpr std::size_t VALUE_SIZE_BYTES = 2 * (1 << 10);
using KeyType = std::array<std::byte, KEY_SIZE_BYTES>;
using ValueType = std::vector<std::byte>;
using ByteType = std::byte;

} // namespace kvaaas

template <> struct std::hash<kvaaas::KeyType> {
  std::size_t operator()(const kvaaas::KeyType &k) const {
    using std::hash;
    using std::size_t;
    using std::string;

    // Compute individual hash values for first,
    // second and third and combine them using XOR
    // and bit shifting:
    return XXH32(k.data(), k.size(), 0);
  }
};
