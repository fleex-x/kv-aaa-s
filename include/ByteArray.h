#pragma once
#include "Core.h"
#include <fstream>
#include <vector>

namespace kvaaas {

class ByteArray {
public:
  virtual void append(const std::vector<ByteType> &bytes) = 0;

  virtual std::vector<ByteType>
  read(std::size_t l, std::size_t r) = 0; //[l, r) -- semi-interval
  virtual void rewrite(std::size_t begin,
                       const std::vector<ByteType> &bytes) = 0;
  virtual std::size_t size() = 0;

  virtual ~ByteArray() = default;
};

class RAMByteArray : public ByteArray {
private:
  std::vector<ByteType> byte_array;

public:
  void append(const std::vector<ByteType> &bytes) override;

  std::vector<ByteType> read(std::size_t l, std::size_t r) override;

  void rewrite(std::size_t begin, const std::vector<ByteType> &bytes) override;

  std::size_t size() override;
};

class FileByteArray final : public ByteArray {
public:
  explicit FileByteArray(const std::string &s, bool withRAII = false);

  void append(const std::vector<ByteType> &bytes) override;

  std::vector<ByteType> read(std::size_t l,
                             std::size_t r) override; //[l, r) -- semi-interval
  void rewrite(std::size_t begin, const std::vector<ByteType> &bytes) override;

  std::size_t size() override;

  ~FileByteArray() override;

  std::string file_name() const noexcept { return underlying_file; }

private:
  std::fstream data;
  std::string underlying_file;
  const bool RAII; // REMOVE THIS!!!
};

using ByteArrayPtr = ByteArray *;
using FileByteArrayPtr = FileByteArray *;

} // namespace kvaaas
