#include "Kvaaas.h"
#include <chrono>
#include <iostream>

using namespace kvaaas;

// usage: %program% SCRIPT_TO_EXECUTE EXAMPLE_TO_COMPARE
int main(const int argc, const char **argv) {
    if (argc < 3) {
        std::cout << "Usage: %program% SCRIPT_TO_EXECUTE EXAMPLE_TO_COMPARE"
                  << std::endl;
        return 1;
    }

    KvaaasOption little_on_disk{
            true, ManagerType::FileMM,
            2,    // log max size
            4,    // skip list max size
            2000, // sst max size
            0.5,
            3 // kvaaas_cnt
    };

    Kvaaas kvs("end2end-dir", little_on_disk);
    std::size_t cnt_queries;
    std::ifstream in(argv[1]);
    std::ifstream expected_answers(argv[2]);
    in >> cnt_queries;
    bool is_ok = true;
    std::size_t cnt_get_queries = 0;
    for (std::size_t i = 0; i < cnt_queries; ++i) {
        std::string command_type;
        in >> command_type;
        if (command_type == "ADD") {
            std::string key_s;
            in >> key_s;
            std::string value_s;
            in >> value_s;
            KeyType key{};
            for (std::size_t j = 0; j < key_s.size(); ++j) {
                key[j] = ByteType(key_s[j]);
            }
            ValueType value(value_s.size());
            for (std::size_t j = 0; j < value_s.size(); ++j) {
                value[j] = ByteType(value_s[j]);
            }
            kvs.add(key, value);
        }
        if (command_type == "REMOVE") {
            std::string key_s;
            in >> key_s;
            KeyType key{};
            for (std::size_t j = 0; j < key_s.size(); ++j) {
                key[j] = ByteType(key_s[j]);
            }
            kvs.remove(key);
        }
        if (command_type == "GET") {
            ++cnt_get_queries;
            std::string key_s;
            in >> key_s;
            KeyType key{};
            for (std::size_t j = 0; j < key_s.size(); ++j) {
                key[j] = ByteType(key_s[j]);
            }
            auto opt_value = kvs.get(key);
            std::string value_s;
            if (opt_value.has_value()) {
                const auto &value = opt_value.value().second;
                value_s.resize(value.size());
                for (std::size_t j = 0; j < value.size(); ++j) {
                    value_s[j] = static_cast<char>(value[j]);
                }
            } else {
                value_s = "key_not_in_storage";
            }
            std::string expected_value_s;
            expected_answers >> expected_value_s;
            if (value_s != expected_value_s) {
                is_ok = false;
                std::cout << i + 1 << " test failed (" << cnt_get_queries << " GET query): key is " << key_s << "\n\t expected: " << expected_value_s << "\n\t  but got: " << value_s << std::endl;
            }
        }
    }
    if (is_ok) {
        std::cout << "tests passed successfully" << std::endl;
        return 0;
    } else {
        return 1;
    }
}
