#pragma once
#include "Core.h"
#include "Shard.h"
#include "xxhash.h"

namespace kvaaas {

struct KvaaasOption {
    const bool force_create;
    const ManagerType type;
    const std::size_t log_max_size;
    const std::size_t sl_max_size;
    const std::size_t sst_max_size;
    const double busy_coeff;
    const std::size_t shard_cnt;
};

inline KvaaasOption DefaultOnDisk = {true, ManagerType::FileMM, 1000, 26'200, 262'000, .65, 10};

class ShardContainer {
private:
    std::size_t size;
    std::size_t cur_pos = 0;
    Shard *buf = nullptr;
public:
    explicit ShardContainer(std::size_t size_) : size(size_) {
        buf = std::allocator<Shard>().allocate(size);
    }

    void emplace_back(std::string root, ShardOption opt) {
        new (buf + cur_pos) Shard(std::move(root), opt);
        ++cur_pos;
    }

    Shard &operator[](std::size_t ind) {
        return buf[ind];
    }

    ~ShardContainer() {
        for (std::size_t i = 0; i < cur_pos; ++i) {
            buf[i].~Shard();
        }
        std::allocator<Shard>().deallocate(buf, size);
    }
};

class Kvaaas {
private:
    ShardContainer shards;
    std::string root;
    KvaaasOption opt;


    [[nodiscard]] std::uint32_t hash_at(const KeyType &key) const {
        return XXH32(key.data(), key.size(), 0) % opt.shard_cnt;
    }

    Shard &get_shard(const KeyType &key)  {
        return shards[hash_at(key)];
    }

    ShardOption get_shard_option() {
        return ShardOption{opt.force_create, opt.type, opt.log_max_size, opt.sl_max_size, opt.sst_max_size, opt.busy_coeff};
    }

public:
    Kvaaas(std::string root_, KvaaasOption opt_) :
     shards(opt_.shard_cnt),
     root(std::move(root_)),
     opt(opt_) {
        if (opt.type == ManagerType::FileMM) {
            std::filesystem::remove_all(root);
            std::filesystem::create_directory(root);
        }
        for (std::size_t i = 0; i < opt.shard_cnt; ++i) {
            auto shard_dir = root + "/" + std::to_string(i);
            if (opt.type == ManagerType::FileMM) {
                std::filesystem::create_directory(shard_dir);
            }
            shards.emplace_back(std::move(shard_dir), get_shard_option());
        }
    }

    void add(const KeyType &key, const ValueType &value) {
        get_shard(key).add(key, value);
    }

    void remove(const KeyType &key) {
        get_shard(key).remove(key);
    }

    std::optional<std::pair<KeyType, ValueType>> get(const KeyType &key) {
        return get_shard(key).get(key);
    }
};

}
