#include "Core.h"
#include "MemoryManager.h"

#include "doctest.h"

namespace {
using namespace kvaaas;
TEST_CASE("Just Creates") {
  FileMemoryManager f("fmm_test");
  auto a = f.create_file(MemoryPurpose::KVS);
  auto b = f.create_file(MemoryPurpose::SST);
  auto c = f.create_file(MemoryPurpose::SKIP_LIST_BL);
  CHECK(a == f.get_file(MemoryPurpose::KVS));
  CHECK(b == f.get_file(MemoryPurpose::SST));
  CHECK(c == f.get_file(MemoryPurpose::SKIP_LIST_BL));
}

TEST_CASE("Check restore") {

}

} // namespace
