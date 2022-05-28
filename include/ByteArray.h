#pragma once
#include <vector>

class ByteArray {
public:
    virtual void append(const std::vector<unsigned char> &bytes) = 0;
    virtual std::vector<unsigned char> read(int l, int r) = 0; //[l, r) -- semi-interval
    virtual std::size_t size() = 0;

    virtual ~ByteArray() = default;
};


class RAMByteArray : public  ByteArray {
private:
    std::vector<unsigned char> byte_array;
public:

    void append(const std::vector<unsigned char> &bytes) override {
        byte_array.insert(byte_array.end(), bytes.begin(), bytes.end());
    }

    std::vector<unsigned char> read(int l, int r) override {
        return {byte_array.begin() + l, byte_array.begin() + r};
    }

    std::size_t size() override {
        return byte_array.size();
    }

};
