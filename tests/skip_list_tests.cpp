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
    for (ByteType &byte : record.key) {
        byte = static_cast<ByteType>('a');
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
        for (ByteType &j : records[i].key) {
            j = static_cast<ByteType>(rnd() % (UCHAR_MAX + 1));
        }
    }
    for (const auto &record : records) {
        viewer.append_record(record);
    }
    for (std::size_t i = 0; i < SIZE; ++i) {
        CHECK(viewer.get_record(i + 1) == records[i]);
    }

    CHECK(viewer.get_head() == 100);
}

TEST_CASE("SLUpperLevelRecordViewer tests") {
    RAMByteArray arr;
    RAMByteArray heads_arr;
    SLUpperLevelRecordViewer viewer(&arr, &heads_arr);
    static constexpr std::size_t SIZE = 100;
    std::vector<uint64_t> heads(SIZE);
    std::random_device rd;
    std::mt19937_64 rnd(rd());
    for (std::size_t i = 0; i < SIZE; ++i) {
        heads[i] = rnd();
        viewer.append_head(heads[i]);
    }

    for (std::size_t i = 0; i < SIZE; ++i) {
        CHECK(heads[i] == viewer.get_head(i));
    }


    SLUpperLevelRecord record;
    record.next = 100;
    record.down = 100;
    record.offset = 100;
    for (ByteType &byte : record.key) {
        byte = static_cast<ByteType>('a');
    }
    viewer.append_record(record);
    CHECK(record == viewer[0]);

    std::vector<SLUpperLevelRecord> records(SIZE);
    for (std::size_t i = 0; i < SIZE; ++i) {
        records[i].next = rnd();
        records[i].down = rnd();
        records[i].offset = rnd();
        for (ByteType &j : records[i].key) {
            j = static_cast<ByteType>(rnd() % (UCHAR_MAX + 1));
        }
    }
    for (const auto &record : records) {
        viewer.append_record(record);
    }
    for (std::size_t i = 0; i < SIZE; ++i) {
        CHECK(viewer[i + 1] == records[i]);
    }

    for (std::size_t i = 0; i < SIZE; ++i) {
        heads[i] = rnd();
        viewer.set_head(i, heads[i]);
    }

    for (std::size_t i = 0; i < SIZE; ++i) {
        CHECK(heads[i] == viewer.get_head(i));
    }
}
