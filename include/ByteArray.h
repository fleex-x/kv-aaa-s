#pragma once
#include <vector>

class ByteArray {
public:
    virtual void append(const std::vector<char> &bytes) = 0;
    virtual std::vector<char> read(int l, int r) = 0; //[l, r) -- semi-interval
};
