#pragma once
#include <vector>
#include <optional>
#include <cassert>
#include <map>
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

    [[nodiscard]] bool has_sst_level() const;

};

bool cmp_memory_type(MemoryType memory_type1, MemoryType memory_type2);
using cmp_memory_type_type = bool(*)(MemoryType, MemoryType);

class MemoryManager {
public:
    virtual ByteArrayPtr get_file(MemoryPurpose memory_purpose, std::optional<std::size_t> sst_level) = 0;

    virtual ByteArrayPtr create_file(MemoryPurpose memory_purpose, std::optional<std::size_t> sst_level) = 0;

    virtual ByteArrayPtr start_overwrite(MemoryPurpose memory_purpose, std::optional<std::size_t> sst_level) = 0;

    virtual void end_overwrite(MemoryPurpose memory_purpose, std::optional<std::size_t> sst_level) = 0;

    virtual void remove(MemoryPurpose memory_purpose, std::optional<std::size_t> sst_level) = 0;

    virtual ByteArrayPtr get_file(MemoryPurpose memory_purpose) = 0;

    virtual ByteArrayPtr create_file(MemoryPurpose memory_purpose) = 0;

    virtual ByteArrayPtr start_overwrite(MemoryPurpose memory_purpose) = 0;

    virtual void end_overwrite(MemoryPurpose memory_purpose) = 0;

    virtual void remove(MemoryPurpose memory_purpose) = 0;

    virtual ~MemoryManager() = default;
};



class RAMMemoryManager : public MemoryManager {
private:
    std::map<MemoryType, ByteArrayPtr, cmp_memory_type_type> memory{cmp_memory_type};
    std::map<MemoryType, ByteArrayPtr, cmp_memory_type_type> memory_to_overwrite{cmp_memory_type};
public:
    RAMMemoryManager() = default;
    RAMMemoryManager(const RAMMemoryManager &) = delete;
    RAMMemoryManager &operator=(const RAMMemoryManager &) = delete;
    RAMMemoryManager(RAMMemoryManager &&) = default;
    RAMMemoryManager &operator=(RAMMemoryManager &&) = default;


    ByteArrayPtr get_file(MemoryPurpose memory_purpose, std::optional<std::size_t> sst_level) override;

    ByteArrayPtr create_file(MemoryPurpose memory_purpose, std::optional<std::size_t> sst_level) override;

    ByteArrayPtr start_overwrite(MemoryPurpose memory_purpose, std::optional<std::size_t> sst_level) override;

    void end_overwrite(MemoryPurpose memory_purpose, std::optional<std::size_t> sst_level) override;

    void remove(MemoryPurpose memory_purpose, std::optional<std::size_t> sst_level) override;


    ByteArrayPtr get_file(MemoryPurpose memory_purpose) override;

    ByteArrayPtr create_file(MemoryPurpose memory_purpose) override;

    ByteArrayPtr start_overwrite(MemoryPurpose memory_purpose) override;

    void end_overwrite(MemoryPurpose memory_purpose) override;

    void remove(MemoryPurpose memory_purpose) override;


    ~RAMMemoryManager() noexcept override;

};

}