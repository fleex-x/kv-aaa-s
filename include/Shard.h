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

// TODO
// If FileMM then write log to json in dtor
// and read in destructor
// Better to add logic of choosing memory manager etc to private function

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
        manager->get_or_create_byte_array(MemoryPurpose::SKIP_LIST_UL),
        manager->get_or_create_byte_array(MemoryPurpose::SKIP_LIST_UL_H));
    skip_list.emplace(sl_bottom_viewer, sl_upper_viewer);
    sst.emplace(SSTRecordViewer(
        manager->get_or_create_byte_array(MemoryPurpose::SST), RebuildSSTRV{}));
  }

  void add(const KeyType &key, const ValueType &value) {
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
      log.clear(); // TODO write to json
    }

    if (skip_list->size() > opt.sl_max_size) {
      auto bytes_for_new_sst = manager->start_overwrite(MemoryPurpose::SST);
      auto view_for_new_sst = SSTRecordViewer{bytes_for_new_sst, NewSSTRV{}};
      auto new_sst =
          SST::merge_into_sst(skip_list->begin(), skip_list->end(),
                              sst->begin(), sst->end(), view_for_new_sst);
      manager->end_overwrite(MemoryPurpose::SST);
      sst.emplace(new_sst);
      auto sl_u = manager->start_overwrite(MemoryPurpose::SKIP_LIST_UL);
      auto sl_u_h = manager->start_overwrite(MemoryPurpose::SKIP_LIST_UL_H);
      auto sl_b = manager->start_overwrite(MemoryPurpose::SKIP_LIST_BL);

      auto sl_upper_viewer = SLUpperLevelRecordViewer(sl_u, sl_u_h);
      auto sl_bottom_viewer = SLBottomLevelRecordViewer(sl_b);

      skip_list.emplace(sl_bottom_viewer, sl_upper_viewer);

      manager->end_overwrite(MemoryPurpose::SKIP_LIST_UL);
      manager->end_overwrite(MemoryPurpose::SKIP_LIST_UL_H);
      manager->end_overwrite(MemoryPurpose::SKIP_LIST_BL);
    }

    // TODO recalc stats
    bool rebuild = false; // TODO somehow calculate to rebuild
    if (rebuild) {
      do_rebuild();
    }
  }

  void remove(const KeyType &key) {
    std::optional<std::uint64_t> offset = get_offset(key);
    if (offset) {
      kvs_viewer->mark_as_deleted(*offset);
    }
  }

  std::optional<std::pair<KeyType, ValueType>> get(const KeyType &key) {
    std::optional<std::uint64_t> offset = get_offset(key);
    if (offset) {
      auto rec = kvs_viewer->read_record(*offset);
      if (rec.is_deleted == std::byte(0))
        return std::pair{rec.key, rec.value};
    } else {
    }
    return std::nullopt;
  }

private:
  void do_rebuild() {}
  std::optional<std::uint64_t> get_offset(const KeyType &key) {
    std::optional<std::uint64_t> offset = log.get_offset(key);
    if (offset) {
      return offset;
    }
    offset = skip_list->find(key);
    if (offset) {
      return offset;
    }
    if (sst->contains(key)) {
      return sst->find_offset(key);
    }
    return std::nullopt;
  }

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
