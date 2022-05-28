#pragma once
#include <vector>
#include <optional>
#include <cassert>
#include "ByteArray.h"

namespace kvaaas {

enum class MemoryPurpose {
    KVS = 0,
    SST = 1,
    SKIP_LIST_UL = 2,
    SKIP_LIST_BL = 3
};

class MemoryType {
private:
    MemoryPurpose mp;
    std::optional<std::size_t> sst_level;
public:
    explicit MemoryType(MemoryPurpose mp_, std::optional<std::size_t> sst_level_ = {});

    [[nodiscard]] MemoryPurpose get_memory_purpose() const;

    [[nodiscard]] std::size_t get_sst_level() const;

};

class MemoryManager {
public:
    virtual ByteArray *get_file(MemoryType ft) = 0;

    virtual ByteArray *create_file(MemoryType ft) = 0;

    virtual ByteArray *start_overwrite(MemoryType ft) = 0;

    virtual void end_overwrite(MemoryType ft) = 0;

    virtual void remove(MemoryType ft) = 0;

    virtual ~MemoryManager() = default;
};

}