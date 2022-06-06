for file in $(find "$1" -iname '*.cpp' -or -iname '*.h' -and -not -iname 'doctest.h'); do clang-format -i "$file"; done
