#pragma once

#include <memory>
#include <optional>
#include <string>

#include "KVSRecordsViewer.h"
#include "Log.h"
#include "MemoryManager.h"
#include "SST.h"
#include "SkipList.h"

namespace kvaaas {

enum class ManagerType {
  FileMM,
  RAMMM,
};

// TODO read from config
struct ShardOption {
  const bool force_create;
  const ManagerType type;
  const std::size_t log_max_size;
  const std::size_t sl_max_size;
  const std::size_t sst_max_size;
  const float busy_coef;
};

struct Shard {
  explicit Shard(std::string root, ShardOption opt)
      : opt(std::move(opt)), root(root) {
    if (opt.type == ManagerType::FileMM) {
      if (opt.force_create) {
        // it creates empty manifest
        manager = std::make_unique<FileMemoryManager>(root);
      } else {
        manager = std::make_unique<FileMemoryManager>(
            FileMemoryManager::from_dir(root));
      }
    } else {
      // TODO maybe process somehow better ?
      manager = std::make_unique<RAMMemoryManager>();
    }
    kvs_viewer = KVSRecordsViewer(
        manager->get_or_create_byte_array(MemoryPurpose::KVS), nullptr);
    SLBottomLevelRecordViewer sl_bottom_viewer(
        manager->get_or_create_byte_array(MemoryPurpose::SKIP_LIST_BL));
    SLUpperLevelRecordViewer sl_upper_viewer(
        manager->get_or_create_byte_array(MemoryPurpose::SKIP_LIST_UL_H),
        manager->get_or_create_byte_array(MemoryPurpose::SKIP_LIST_UL_H));
    skip_list.emplace(sl_bottom_viewer, sl_upper_viewer);
    sst.emplace(SSTRecordViewer(
        manager->get_or_create_byte_array(MemoryPurpose::SST), RebuildSSTRV{}));
  }

  void add(KeyType key, ValueType value) {
    // TODO 
    // please, fix calculating stat !!!
    
    // Step 1 -- write into KVS 
    KVSRecord rec;
    rec.value = std::move(value);
    rec.key = key;
    rec.is_deleted = std::byte(0);
    rec.value_size = rec.value.size();
    auto offset = kvs_viewer->append(std::move(rec));

    // Step 2 -- into log 
    log.add(key, offset);

    if (log.size() > opt.log_max_size) {
      skip_list->push_from(log.begin(), log.end());
      log.clear();
    }


  }

private:
  ShardOption opt;
  std::string root;
  std::unique_ptr<MemoryManager> manager;
  Log log{};
  std::optional<KVSRecordsViewer> kvs_viewer;
  std::optional<SkipList> skip_list;
  std::optional<struct SST> sst;
  struct RebuildStat {
    unsigned total = 0;
    unsigned bad = 0;
  };
  RebuildStat stat{};
};
} // namespace kvaaas
