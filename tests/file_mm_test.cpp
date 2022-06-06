#include "Core.h"
#include "MemoryManager.h"

#include "doctest.h"

#include <filesystem>
#include <iostream> // see later
namespace {

constexpr bool CREATE_N_DELETE_DIRS = true;

void create_dir(const std::string &dir) {
  if (CREATE_N_DELETE_DIRS) {
    std::filesystem::create_directory(dir);
  }
}

void remove_dir(const std::string &dir) {
  if (CREATE_N_DELETE_DIRS) {
    std::filesystem::remove_all(dir);
  }
}

struct RAIDir {
  RAIDir(std::string s) : name(std::move(s)) { create_dir(name); }

  ~RAIDir() { remove_dir(name); }

private:
  std::string name;
};

using namespace kvaaas;
TEST_CASE("Just Creates") {
  RAIDir _("fmm_test");
  FileMemoryManager f("fmm_test");
  auto a = f.create_byte_array(MemoryPurpose::KVS);
  auto b = f.create_byte_array(MemoryPurpose::SST);
  auto c = f.create_byte_array(MemoryPurpose::SKIP_LIST_BL);
  CHECK(a == f.get_byte_array(MemoryPurpose::KVS));
  CHECK(b == f.get_byte_array(MemoryPurpose::SST));
  CHECK(c == f.get_byte_array(MemoryPurpose::SKIP_LIST_BL));
}

TEST_CASE("Check restore") {

  std::vector v1 = {std::byte(4), std::byte(2)};
  std::vector v2 = {std::byte(6), std::byte(6)};
  std::vector v3 = {std::byte(1), std::byte(3)};
  {

    create_dir("fmm_test2");

    FileMemoryManager f("fmm_test2");
    auto a = f.create_byte_array(MemoryPurpose::KVS);
    auto b = f.create_byte_array(MemoryPurpose::SST);
    auto c = f.create_byte_array(MemoryPurpose::SKIP_LIST_BL);

    a->append(v1);
    b->append(v2);
    c->append(v3);
  }
  FileMemoryManager f2 = FileMemoryManager::from_dir("fmm_test2");

  auto a2 = f2.get_byte_array(MemoryPurpose::KVS);
  auto b2 = f2.get_byte_array(MemoryPurpose::SST);
  auto c2 = f2.get_byte_array(MemoryPurpose::SKIP_LIST_BL);

  CHECK(a2->read(0, 2) == v1);
  CHECK(b2->read(0, 2) == v2);
  CHECK(c2->read(0, 2) == v3);
  remove_dir("fmm_test2");
}

TEST_CASE("Overwrite") {
  RAIDir _("fmm_test3");
  FileMemoryManager manager("fmm_test3");
  auto sst_old = manager.create_byte_array(MemoryPurpose::SST);

  sst_old->append(std::vector<std::byte>(100, std::byte(0)));

  ByteArrayPtr new_sst = manager.start_overwrite(MemoryPurpose::SST);

  new_sst->append({std::byte(13), std::byte(42)});
  manager.end_overwrite(MemoryPurpose::SST);
  CHECK(new_sst == manager.get_byte_array(MemoryPurpose::SST));
  CHECK(new_sst->read(1, 2) == std::vector{std::byte(42)});

  new_sst = manager.start_overwrite(MemoryPurpose::SST);
  new_sst->append(
      std::vector<std::byte>({std::byte(6), std::byte(7), std::byte(8)}));
  manager.end_overwrite(MemoryPurpose::SST);
  CHECK(new_sst == manager.get_byte_array(MemoryPurpose::SST));
  CHECK(new_sst->read(0, 3) ==
        std::vector<std::byte>({std::byte(6), std::byte(7), std::byte(8)}));
}

TEST_CASE("Overwrite + restore") {
  RAIDir _("fmm_test4");
  {
    FileMemoryManager manager("fmm_test4");
    auto sst_old = manager.create_byte_array(MemoryPurpose::SST);
    sst_old->append(std::vector<std::byte>(100, std::byte(0)));

    auto new_sst = manager.start_overwrite(MemoryPurpose::SST);
    new_sst->append(std::vector<std::byte>(1000, std::byte(42)));
    manager.end_overwrite(MemoryPurpose::SST);
  }

  FileMemoryManager manager = FileMemoryManager::from_dir("fmm_test4");
  auto sst = manager.get_byte_array(MemoryPurpose::SST);
  CHECK(sst->read(0, 100) == std::vector<std::byte>(100, std::byte(42)));
}
} // namespace
