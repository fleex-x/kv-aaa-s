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

class Kvaaas {
private:
    std::vector<Shard> shards;
    std::string root;
    KvaaasOption opt;

    ShardOption get_shard_option() {

    }

public:
    Kvaaas(std::string root_, KvaaasOption opt) :
     root(std::move(root_)),
     opt(opt) {
        shards.reserve(opt.shard_cnt);
        for (std::size_t i = 0; i < opt.shard_cnt; ++i) {
            shards.emplace_back(root, ShardOption{opt.force_create, opt.type, opt.log_max_size, opt.sl_max_size, opt.sst_max_size, opt.busy_coeff});
        }
    }

    void add(const KeyType &key, const ValueType &value) {

    }

    void remove(const KeyType &key) {

    }

    std::optional<std::pair<KeyType, ValueType>> get(const KeyType &key) {

    }


};

}