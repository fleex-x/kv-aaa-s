cmake_minimum_required(VERSION 3.10)

project(kv-aaa-s CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

file(GLOB zstd_common "libs/zstd/common/*.h" "libs/zstd/common/*.c")
file(GLOB zstd_compress "libs/zstd/compress/*.h" "libs/zstd/compress/*.c")
file(GLOB zstd_decompress "libs/zstd/decompress/*.h" "libs/zstd/decompress/*.c" "zstd/decompress/*.S")
file(GLOB zstd_dict_builder "libs/zstd/dictBuilder/*.h" "libs/zstd/dictBuilder/*.c")

file(GLOB srcs "src/*.cpp")
file(GLOB tests "tests/*.cpp")
file(GLOB headers "include/*.h")
file(GLOB lib_headers "libs/include/*.h")
file(GLOB lib_cpp_srcs "libs/src/*.cpp")

set(zstd ${zstd_common} ${zstd_compress} ${zstd_decompress} ${zstd_dict_builder})

set(CMAKE_CXX_FLAGS "-Wall -Wextra -pedantic") # TODO add -Werror


include_directories(libs/include)
include_directories(include)

add_executable(main main.cpp)

add_executable(test ${tests} ${srcs} ${headers} ${lib_headers})
add_executable(KVSRecordsTest tests/doctest_main.cpp tests/KVSRecords_test.cpp tests/doctest.h ${srcs} ${headers} ${lib_headers})
