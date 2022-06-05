#pragma once

#include "Core.h"
#include <unordered_map>

#include <optional>

namespace kvaaas {
struct Log {
  std::size_t size() const noexcept { return _map.size(); }
  void add(const KeyType &key, std::uint64_t offset) {
    _map.emplace(key, offset);
  }

  void remove(const KeyType &key) { _map.erase(key); }

  std::optional<std::uint64_t> get_offset(const KeyType &key) {
    auto it = _map.find(key);
    if (it == _map.end()) {
      return std::nullopt;
    }
    return it->second;
  }

  void clear() { _map.clear(); }

  auto begin() { return _map.begin(); }

  auto end() { return _map.end(); }

  auto cbegin() const { return _map.cbegin(); }

  auto cend() const { return _map.cend(); }

private:
  std::unordered_map<KeyType, std::uint64_t> _map;
};
} // namespace kvaaas
