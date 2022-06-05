#include "Core.h"
#include "Log.h"

#include "doctest.h"

#include <algorithm>
#include <unordered_map>

namespace kvaaas {
TEST_CASE("JustWorks") {
  std::unordered_map<KeyType, std::uint64_t> mapa;
  Log log;
  std::byte z = std::byte{0};
  mapa[KeyType{z, z, z}] = 2;
  log.add(KeyType{z, z, z}, 2);
  CHECK(std::equal(log.begin(), log.end(), mapa.begin(), mapa.end()));
}
} // namespace kvaaas
