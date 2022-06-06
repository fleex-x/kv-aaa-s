for file in $(find tests/ -iname '*.cpp' -or -iname '*.h' -and -not -iname 'doctest.h'); do clang-format -i "$file"; done
