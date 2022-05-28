#include "doctest.h"
#include "FileManager.h"
#include <memory>

using namespace kvaaas;

TEST_CASE("Simple test") {
    FileType ft(FilePurpose::SST, 4);
    CHECK(ft.get_sst_level() == 4);
}

TEST_CASE("RAMByteArray") {
    RAMByteArray ram_arr;
    ByteArray &arr = ram_arr;
    CHECK(arr.size() == 0);
    arr.append({0, 1, 2, 3});
    CHECK(arr.size() == 4);
    CHECK(arr.read(0, 1) == std::vector<unsigned char>({0}));
    CHECK(arr.read(0, 4) == std::vector<unsigned char>({0, 1, 2, 3}));
    CHECK(arr.read(3, 4) == std::vector<unsigned char>({3}));
    CHECK(arr.read(3, 4) == std::vector<unsigned char>({3}));
    arr.append({3, 4, 5});
    CHECK(arr.size() == 7);
    CHECK(arr.read(3, 7) == std::vector<unsigned char>({3, 3, 4, 5}));
}