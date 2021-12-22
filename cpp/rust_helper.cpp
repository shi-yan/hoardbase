#include "rust_helper.h"
#include <iostream>
#include <nlohmann/json.hpp>

void test_print() {
    std::cout << "Hello, World! << test print" << std::endl;
    nlohmann::json j = {
        {"hello", "world"},
        {"number", 1},
        {"array", {1, 2, 3, 4}},
        {"object", {
            {"key", "value"},
        }},
    };

    std::cout << "Hello, world! print using cpp called by rust\n" << j.dump(4) << std::endl;
}