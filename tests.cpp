#include "doctest.h"
#include "FileManager.h"

TEST_CASE("Simple test") {
    FileType ft(FilePurpose::SST, 4);
    CHECK(ft.get_sst_level() == 3);
}