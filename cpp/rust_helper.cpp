#include "rust_helper.h"
#include <iostream>
#include <nlohmann/json.hpp>

void test_print()
{
    std::cout << "Hello, World! << test print" << std::endl;
    nlohmann::json j = {
        {"hello", "world"},
        {"number", 1},
        {"array", {1, 2, 3, 4}},
        {"object", {
                       {"key", "value"},
                   }},
    };

    std::cout << "Hello, world! print using cpp called by rust\n"
              << j.dump(4) << std::endl;
}

void *create_json()
{
    nlohmann::json *j = new nlohmann::json();
    return reinterpret_cast<void *>(j);
}

void free_json(void *json_ptr)
{
    nlohmann::json *j = reinterpret_cast<nlohmann::json *>(json_ptr);
    delete j;
}

void print_json(void *json_ptr)
{
    nlohmann::json *j = reinterpret_cast<nlohmann::json *>(json_ptr);
    std::cout << j->dump(4) << std::endl;
}

void insert_i64(void *json_ptr, const char *key, int64_t value)
{
    nlohmann::json *j = reinterpret_cast<nlohmann::json *>(json_ptr);
    (*j)[key] = value;
}

void insert_f64(void *json_ptr, const char *key, double value)
{
    nlohmann::json *j = reinterpret_cast<nlohmann::json *>(json_ptr);
    (*j)[key] = value;
}

void insert_null(void *json_ptr, const char *key)
{
    nlohmann::json *j = reinterpret_cast<nlohmann::json *>(json_ptr);
    (*j)[key] = nullptr;
}

void insert_bool(void *json_ptr, const char *key, bool value)
{
    nlohmann::json *j = reinterpret_cast<nlohmann::json *>(json_ptr);
    (*j)[key] = value;
}

void insert_str(void *json_ptr, const char *key, const char *value)
{
    nlohmann::json *j = reinterpret_cast<nlohmann::json *>(json_ptr);
    (*j)[key] = value;
}

void *create_json_array()
{
    nlohmann::json *j = new nlohmann::json();
    *j = nlohmann::json::array();
    return reinterpret_cast<void *>(j);
}

void array_push_i64(void *json_ptr, int64_t value)
{
    nlohmann::json *j = reinterpret_cast<nlohmann::json *>(json_ptr);
    (*j).push_back(value);
}

void array_push_f64(void *json_ptr, double value)
{
    nlohmann::json *j = reinterpret_cast<nlohmann::json *>(json_ptr);
    (*j).push_back(value);
}

void array_push_null(void *json_ptr)
{
    nlohmann::json *j = reinterpret_cast<nlohmann::json *>(json_ptr);
    (*j).push_back(nullptr);
}

void array_push_bool(void *json_ptr, bool value)
{
    nlohmann::json *j = reinterpret_cast<nlohmann::json *>(json_ptr);
    (*j).push_back(value);
}

void array_push_str(void *json_ptr, const char *value)
{
    nlohmann::json *j = reinterpret_cast<nlohmann::json *>(json_ptr);
    (*j).push_back(value);
}

void array_push_obj(void *json_ptr, void *obj)
{
    nlohmann::json *j = reinterpret_cast<nlohmann::json *>(json_ptr);
    (*j).push_back(*reinterpret_cast<nlohmann::json *>(obj));
}

void insert_obj(void *json_ptr, const char *key, void *obj)
{
    nlohmann::json *j = reinterpret_cast<nlohmann::json *>(json_ptr);
    (*j)[key] = *reinterpret_cast<nlohmann::json *>(obj);
}