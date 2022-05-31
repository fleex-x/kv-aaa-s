#pragma once 

#include <array>

namespace kvaaas {
	constexpr std::size_t KEY_SIZE_BITS = 128; // TODO read in config
	constexpr std::size_t VALUE_SIZE_BYTES = 2 * (1 << 10);
	using KeyType = std::array<std::byte, KEY_SIZE_BITS/8>;
	using ValueType = std::array<std::byte, VALUE_SIZE_BYTES>;
	
}
