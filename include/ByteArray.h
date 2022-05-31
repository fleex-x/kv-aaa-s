#pragma once
#include <vector>

namespace kvaaas {

class ByteArray {
public:
    virtual void append(const std::vector<unsigned char> &bytes) = 0;

    virtual std::vector<unsigned char> read(std::size_t l, std::size_t r) = 0; //[l, r) -- semi-interval
    virtual void rewrite(std::size_t begin, const std::vector<unsigned char> &bytes) = 0;
    virtual std::size_t size() = 0;

    virtual ~ByteArray() = default;
};

using ByteArrayPtr = ByteArray*;


class RAMByteArray : public ByteArray {
private:
    std::vector<unsigned char> byte_array;
public:

    void append(const std::vector<unsigned char> &bytes) override;

    std::vector<unsigned char> read(std::size_t l, std::size_t r) override;

    void rewrite(std::size_t begin, const std::vector<unsigned char> &bytes) override;

    std::size_t size() override;
};

}