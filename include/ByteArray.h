#pragma once
#include "Core.h"
#include <vector>
#include <fstream>

#define BA_TESTMODE true

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

using ByteArrayPtr = ByteArray *;

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
private:
  std::fstream data;

public:

  explicit FileByteArray(const std::string &s);

  void append(const std::vector<ByteType> &bytes) override;

  std::vector<ByteType> read(std::size_t l,
                             std::size_t r) override; //[l, r) -- semi-interval
  void rewrite(std::size_t begin, const std::vector<ByteType> &bytes) override;

  std::size_t size() override;

  ~FileByteArray() override;
};
} // namespace kvaaas