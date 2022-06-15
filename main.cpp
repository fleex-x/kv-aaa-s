#include "MemoryManager.h"
#include "ByteArray.h"
#include "libs/zstd/zstd.h"

int main() {
  ZSTD_compress(NULL, 0, NULL, 0, 7);
}
