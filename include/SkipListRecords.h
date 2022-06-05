#pragma once
#include "ByteArray.h"
#include "Core.h"
#include <cstdint>
#include <limits>


namespace kvaaas {

static constexpr std::uint64_t NULL_NODE = std::numeric_limits<std::uint64_t>::max();

struct SLBottomLevelRecord {
    SLBottomLevelRecord() = default;
    SLBottomLevelRecord(const KeyType &key_, std::uint64_t offset_);
    std::uint64_t next = 0;
    std::uint64_t offset = 0;
    KeyType key{};
    static constexpr std::uint64_t SIZE = sizeof(next) + sizeof(offset) + KEY_SIZE_BYTES;
    static constexpr std::uint64_t NEXT_BEGIN = 0;
    static constexpr std::uint64_t OFFSET_BEGIN = sizeof(next);
    static constexpr std::uint64_t KEY_BEGIN = sizeof(next) + sizeof(offset);

};

bool operator==(const SLBottomLevelRecord &record1,
                const SLBottomLevelRecord &record2);

class SLBottomLevelRecordViewer {
private:
    ByteArrayPtr byte_arr;
    static constexpr std::uint64_t HEAD_SIZE = sizeof(SLBottomLevelRecord::next);
    static constexpr std::uint64_t NULL_NODE = std::numeric_limits<decltype(SLBottomLevelRecord::next)>::max();
    static std::uint64_t get_begin(std::uint64_t ind);
public:
    explicit SLBottomLevelRecordViewer(ByteArrayPtr byte_arr_);
    SLBottomLevelRecord get_record(std::uint64_t ind);
    std::uint64_t get_next(std::uint64_t ind);
    void set_offset(std::uint64_t ind, std::uint64_t new_offset);
    void set_next(std::uint64_t ind, std::uint64_t new_next);
    SLBottomLevelRecord operator[](std::uint64_t ind);
    bool has_head();
    std::uint64_t get_head();
    std::uint64_t append_record(const SLBottomLevelRecord &record);
    void set_head(std::uint64_t head);
};

struct SLUpperLevelRecord {
    SLUpperLevelRecord() = default;
    SLUpperLevelRecord(const KeyType &key_, std::uint64_t  down_, std::uint64_t offset_);
    std::uint64_t next = 0;
    std::uint64_t down = 0;
    std::uint64_t offset = 0;
    KeyType key{};
    static constexpr std::uint64_t SIZE = sizeof(next) +  sizeof(down) + sizeof(offset) + KEY_SIZE_BYTES;
    static constexpr std::uint64_t NEXT_BEGIN = 0;
    static constexpr std::uint64_t DOWN_BEGIN = sizeof(down);
    static constexpr std::uint64_t OFFSET_BEGIN = sizeof(down) + sizeof(next);
    static constexpr std::uint64_t KEY_BEGIN = sizeof(down) + sizeof(next) + sizeof(offset);

};

bool operator==(const SLUpperLevelRecord &record1,
                const SLUpperLevelRecord &record2);

class SLUpperLevelRecordViewer {
private:
    ByteArrayPtr byte_arr;
    ByteArrayPtr heads;
    static constexpr std::uint64_t HEAD_SIZE = sizeof(SLUpperLevelRecord::next);
    static constexpr std::uint64_t NULL_NODE = std::numeric_limits<decltype(SLUpperLevelRecord::next)>::max();
    static_assert(sizeof(SLUpperLevelRecord::next) == sizeof(SLUpperLevelRecord::down));
    static std::uint64_t get_begin(std::uint64_t ind);
public:
    explicit SLUpperLevelRecordViewer(ByteArrayPtr byte_arr_, ByteArrayPtr heads_);
    std::uint64_t get_levels();
    SLUpperLevelRecord get_record(std::uint64_t ind);
    std::uint64_t get_next(std::uint64_t ind);
    void set_offset(std::uint64_t ind, std::uint64_t new_offset);
    void set_next(std::uint64_t ind, std::uint64_t new_next);
    SLUpperLevelRecord operator[](std::uint64_t ind);
    std::uint64_t append_record(const SLUpperLevelRecord &record);
    std::uint64_t get_head(std::uint64_t list_ind);
    void set_head(std::uint64_t list_ind, std::uint64_t head);
    void append_head(std::uint64_t head);
};

} // namespace kvaaas
