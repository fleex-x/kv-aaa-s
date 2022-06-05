#pragma once

#include "ByteArray.h"
#include "Error.h"
#include <cstdint>
#include <cstring>

namespace kvaaas {

struct KVSRecord {
  KeyType key;
  ByteType is_deleted;
  std::uint64_t value_size;
  ValueType value;
};

class KVSRecordsViewer {
private:
  ByteArrayPtr byte_arr;
  void *comp;

public:
  KVSRecordsViewer() = delete;

  KVSRecordsViewer(ByteArray *arr, void *compressor);

  /* Expect<offset, status>*/ void append(KVSRecord record);

  static std::uint64_t getValueSize(const KVSRecord &record);

  KVSRecord readRecord(uint64_t offset);

  void markAsDeleted(uint64_t offset);

  bool isDeleted(uint64_t offset);
};

bool operator==(const KVSRecord &record1, const KVSRecord &record2);

} // namespace kvaaas
