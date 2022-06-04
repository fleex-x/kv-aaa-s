#include "doctest.h"
#include "SkipListRecords.h"
#include "SkipList.h"
#include "ByteArray.h"
#include <map>
#include <climits>
#include <random>
#include <algorithm>

using namespace kvaaas;

//TEST_CASE("SLBottomLevelRecordViewer tests") {
//    RAMByteArray arr;
//    SLBottomLevelRecordViewer viewer(&arr);
//    CHECK(viewer.get_head() == NULL_NODE);
//    viewer.set_head(100);
//    CHECK(viewer.get_head() == 100);
//    SLBottomLevelRecord record;
//    record.next = 100;
//    record.offset = 100;
//    for (ByteType &byte : record.key) {
//        byte = static_cast<ByteType>('a');
//    }
//    viewer.append_record(record);
//    CHECK(record == viewer.get_record(0));
//
//    static constexpr std::size_t SIZE = 100;
//    std::vector<SLBottomLevelRecord> records(SIZE);
//    std::random_device rd;
//    std::mt19937_64 rnd(rd());
//    for (std::size_t i = 0; i < SIZE; ++i) {
//        records[i].next = rnd();
//        records[i].offset = rnd();
//        for (ByteType &j : records[i].key) {
//            j = static_cast<ByteType>(rnd() % (UCHAR_MAX + 1));
//        }
//    }
//    for (const auto &record : records) {
//        viewer.append_record(record);
//    }
//    for (std::size_t i = 0; i < SIZE; ++i) {
//        CHECK(viewer.get_record(i + 1) == records[i]);
//    }
//
//    CHECK(viewer.get_head() == 100);
//}
//
//TEST_CASE("SLUpperLevelRecordViewer tests") {
//    RAMByteArray arr;
//    RAMByteArray heads_arr;
//    SLUpperLevelRecordViewer viewer(&arr, &heads_arr);
//    static constexpr std::size_t SIZE = 100;
//    std::vector<uint64_t> heads(SIZE);
//    std::random_device rd;
//    std::mt19937_64 rnd(rd());
//    for (std::size_t i = 0; i < SIZE; ++i) {
//        heads[i] = rnd();
//        viewer.append_head(heads[i]);
//    }
//
//    for (std::size_t i = 0; i < SIZE; ++i) {
//        CHECK(heads[i] == viewer.get_head(i));
//    }
//
//
//    SLUpperLevelRecord record;
//    record.next = 100;
//    record.down = 100;
//    record.offset = 100;
//    for (ByteType &byte : record.key) {
//        byte = static_cast<ByteType>('a');
//    }
//    viewer.append_record(record);
//    CHECK(record == viewer[0]);
//
//    std::vector<SLUpperLevelRecord> records(SIZE);
//    for (std::size_t i = 0; i < SIZE; ++i) {
//        records[i].next = rnd();
//        records[i].down = rnd();
//        records[i].offset = rnd();
//        for (ByteType &j : records[i].key) {
//            j = static_cast<ByteType>(rnd() % (UCHAR_MAX + 1));
//        }
//    }
//    for (const auto &record : records) {
//        viewer.append_record(record);
//    }
//    for (std::size_t i = 0; i < SIZE; ++i) {
//        CHECK(viewer[i + 1] == records[i]);
//    }
//
//    for (std::size_t i = 0; i < SIZE; ++i) {
//        heads[i] = rnd();
//        viewer.set_head(i, heads[i]);
//    }
//
//    for (std::size_t i = 0; i < SIZE; ++i) {
//        CHECK(heads[i] == viewer.get_head(i));
//    }
//}

TEST_CASE("SkipList tests simple") {
    RAMByteArray bottom;
    RAMByteArray heads;
    RAMByteArray upper;
    SLBottomLevelRecordViewer bottom_viewer(&bottom);
    SLUpperLevelRecordViewer upper_viewer(&upper, &heads);
    SkipList skip_list(bottom_viewer, upper_viewer);
    auto one = std::byte(1);
    auto two = std::byte(2);
    auto three = std::byte(3);
    auto four = std::byte(4);
    KeyType key1{one, two, three, four};
    skip_list.add(key1, 123);

    auto val = skip_list.find(key1);

    CHECK(val.has_value());
    CHECK(val.value() == 123);
    KeyType key2{one, one, one, one};
    skip_list.add(key2, 345);

    val = skip_list.find(key1);
    CHECK(val.has_value());
    CHECK(val.value() == 123);

    val = skip_list.find(key2);
    CHECK(val.has_value());
    CHECK(val.value() == 345);

    KeyType key3{one, one, three, four};
    skip_list.add(key3, 567);

    val = skip_list.find(key1);
    CHECK(val.has_value());
    CHECK(val.value() == 123);

    val = skip_list.find(key2);
    CHECK(val.has_value());
    CHECK(val.value() == 345);

    val = skip_list.find(key3);
    CHECK(val.has_value());
    CHECK(val.value() == 567);
}


TEST_CASE("SkipList stress-tests") {
    RAMByteArray bottom;
    RAMByteArray heads;
    RAMByteArray upper;
    SLBottomLevelRecordViewer bottom_viewer(&bottom);
    SLUpperLevelRecordViewer upper_viewer(&upper, &heads);
    SkipList skip_list(bottom_viewer, upper_viewer);

    std::random_device rnd_device;
    std::mt19937 mersenne_engine{rnd_device()}; // Generates random integers
    std::uniform_int_distribution<unsigned> dist{
            1, static_cast<unsigned>(std::numeric_limits<std::byte>::max())};

    auto gen_byte = [&dist, &mersenne_engine]() {
        return std::byte(dist(mersenne_engine));
    };
    auto gen_key = [&] {
        KeyType key;
        for (std::size_t i = 0; i < key.size(); ++i) {
            key[i] = gen_byte();
        }
        return key;
    };
    auto gen_offset = [&] { return (std::uint64_t)dist(mersenne_engine); };

    std::vector<std::pair<KeyType, std::uint64_t>> records;
    auto has_key = [&records](const KeyType &key) {
        for (const auto &[key_, _] : records) {;
            if (key == key_) {
                return true;
            }
        }
        return false;
    };
    static constexpr std::size_t UWU = 500;
    for (std::size_t i = 0; i < UWU; ++i) {
        while (true) {
            auto key = gen_key();
            if (!has_key(key)) {
                records.emplace_back(key, gen_offset());
                break;
            }
        }
        skip_list.add(records.back().first, records.back().second);
        for (const auto &[key, offset] : records) {
            auto val = skip_list.find(key);
            CHECK(val.has_value());
            CHECK(val.value() == offset);
        }
    }

}