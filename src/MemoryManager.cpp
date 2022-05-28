#include "MemoryManager.h"
#include <vector>
#include <optional>
#include <cassert>

namespace kvaaas {

MemoryType::MemoryType(MemoryPurpose mp_, std::optional<std::size_t> sst_level_) :
    mp(mp_),
    sst_level(sst_level_) {
    if (mp_ == MemoryPurpose::SST) {
        assert(sst_level.has_value());
    }
}

MemoryPurpose MemoryType::get_memory_purpose() const {
    return mp;
}

std::size_t MemoryType::get_sst_level() const {
    return sst_level.value();
}

}