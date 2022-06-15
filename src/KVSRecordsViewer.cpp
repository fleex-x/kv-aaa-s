#include <KVSRecordsViewer.h>

namespace kvaaas {

KVSRecordsViewer::KVSRecordsViewer(ByteArray *arr, void *compressor)
    : byte_arr(arr), comp(compressor) {}

std::size_t KVSRecordsViewer::append(const KVSRecord &record) {
//  std::uint64_t SIZE = get_value_size(record);
//  std::vector<ByteType> record_chars(SIZE); /// TODO make pretty
//  std::memcpy(record_chars.data(), &record.key, KEY_SIZE_BYTES);
//  std::memcpy(record_chars.data() + KEY_SIZE_BYTES, &record.is_deleted,
//              sizeof(record.is_deleted));
//  std::memcpy(record_chars.data() + KEY_SIZE_BYTES + sizeof(record.is_deleted),
//              &record.value_size, sizeof(record.value_size));
//  std::memcpy(record_chars.data() + KEY_SIZE_BYTES + sizeof(record.is_deleted) +
//                  sizeof(record.value_size),
//              record.value.data(), record.value_size);

  std::vector<ByteType> value(record.value_size * 3/2);
  std::uint64_t size = ZSTD_compress(value.data(), record.value_size * 3/2, record.value.data(), record.value_size, 7);
  auto rec = byte_arr->size();
  byte_arr->append(record.key.data(), record.key.size());
  byte_arr->append(reinterpret_cast<const ByteType *>(&record.is_deleted), sizeof(record.is_deleted));
  byte_arr->append(reinterpret_cast<const ByteType *>(&record.value_size), sizeof(record.value_size));
  byte_arr->append(reinterpret_cast<const ByteType *>(&size), sizeof(size));
  byte_arr->append(value.data(), size);
  return rec;
}

std::uint64_t
KVSRecordsViewer::append_not_deleted_record(const KeyType &key,
                                            const ValueType &value) {
  auto res = byte_arr->size();
  static const ByteType not_deleted{0};
  std::uint64_t value_size = value.size();
  byte_arr->append(key.data(), KEY_SIZE_BYTES);
  byte_arr->append(reinterpret_cast<const ByteType *>(&not_deleted),
                   sizeof(not_deleted));
  byte_arr->append(reinterpret_cast<const ByteType *>(&value_size),
                   sizeof(value_size));
  byte_arr->append(value);
  return res;
}

KVSRecord KVSRecordsViewer::read_record(uint64_t offset) {
  KVSRecord record{};
  std::vector<ByteType> key_array =
      byte_arr->read(offset, offset + KEY_SIZE_BYTES);
  std::memcpy(&record.key, key_array.data(), key_array.size());
  offset += KEY_SIZE_BYTES;

  std::vector<ByteType> isDeleted_array =
      byte_arr->read(offset, offset + sizeof(record.is_deleted));
  std::memcpy(&record.is_deleted, isDeleted_array.data(),
              sizeof(record.is_deleted));
  offset += sizeof(record.is_deleted);

  std::vector<ByteType> value_size_array =
      byte_arr->read(offset, offset + sizeof(record.value_size));
  std::memcpy(&record.value_size, value_size_array.data(),
              sizeof(record.value_size));
  offset += sizeof(record.value_size);

  std::vector<ByteType> size_array =
      byte_arr->read(offset, offset + sizeof(record.compressed_size));
  std::memcpy(&record.compressed_size, size_array.data(),
              sizeof(record.compressed_size));
  offset += sizeof(record.compressed_size);

  std::vector<ByteType> in(record.compressed_size);
  std::vector<ByteType> out(record.value_size);
  byte_arr->read_ptr(in.data(), offset, offset + record.compressed_size);
  ZSTD_decompress(out.data(), record.value_size, in.data(), record.compressed_size);
  record.value = std::move(out);
  return record;
}

void KVSRecordsViewer::mark_as_deleted(uint64_t offset) {
  byte_arr->rewrite(offset + KEY_SIZE_BYTES,
                    std::vector<ByteType>(1, ByteType{1}));
}

bool KVSRecordsViewer::is_deleted(uint64_t offset) {
  return byte_arr->read(offset + KEY_SIZE_BYTES,
                        offset + KEY_SIZE_BYTES + 1)[0] == ByteType{1};
}

std::uint64_t KVSRecordsViewer::get_value_size(const KVSRecord &record) {
  return KEY_SIZE_BYTES + sizeof(record.is_deleted) +
         sizeof(record.value_size) + sizeof(record.compressed_size) + record.compressed_size;
}

bool operator==(const KVSRecord &record1, const KVSRecord &record2) {
  return record1.key == record2.key &&
         record1.is_deleted == record2.is_deleted &&
         record1.value_size == record2.value_size &&
         record1.value == record2.value;
}

} // namespace kvaaas
