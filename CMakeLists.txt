cmake_minimum_required(VERSION 3.10)

project(kv-aaa-s CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

file(GLOB zstd_common "zstd/common/*.h" "zstd/common/*.c")
file(GLOB zstd_compress "zstd/compress/*.h" "zstd/compress/*.c")
file(GLOB zstd_decompress "zstd/decompress/*.h" "zstd/decompress/*.c" "zstd/decompress/*.S")
file(GLOB zstd_dict_builder "zstd/dictBuilder/*.h" "zstd/dictBuilder/*.c")

file(GLOB srcs "src/*.cpp")

set(zstd ${zstd_common} ${zstd_compress} ${zstd_decompress} ${zstd_dict_builder})

set(CMAKE_CXX_FLAGS "-Wall -Wextra -pedantic") # TODO add -Werror

include_directories(include)

add_executable(main main.cpp)

add_executable(test doctest_main.cpp tests.cpp ${srcs})