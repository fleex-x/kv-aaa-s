#include "doctest.h"
#include "SkipListRecords.h"
#include "ByteArray.h"
#include <climits>
#include <random>
#include <iostream>

using namespace kvaaas;

TEST_CASE("SLBottomLevelRecordViewer tests") {
    RAMByteArray arr;
    SLBottomLevelRecordViewer viewer(&arr);
    CHECK(viewer.get_head() == 0);
    viewer.set_head(100);
    CHECK(viewer.get_head() == 100);
    SLBottomLevelRecord record;
    record.next = 100;
    record.offset = 100;
    for (unsigned char &byte : record.key) {
        byte = 'a';
    }
    viewer.append_record(record);
    CHECK(record == viewer.get_record(0));

    static constexpr std::size_t SIZE = 100;
    std::vector<SLBottomLevelRecord> records(SIZE);
    std::random_device rd;
    std::mt19937_64 rnd(rd());
    for (std::size_t i = 0; i < SIZE; ++i) {
        records[i].next = rnd();
        records[i].offset = rnd();
        for (unsigned char &j : records[i].key) {
            j = rnd() % (UCHAR_MAX + 1);
        }
    }
    for (const auto &record : records) {
        viewer.append_record(record);
    }
    for (std::size_t i = 0; i < SIZE; ++i) {
        CHECK(viewer.get_record(i + 1).next == records[i].next);
    }

    CHECK(viewer.get_head() == 100);

}
