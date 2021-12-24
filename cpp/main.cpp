#include <hoardbase.h>
#include <iostream>
#include "serde_json.h"
#include <nlohmann/json.hpp>
#include "record.h"

int main()
{
    void *db = hoardbase_open("test.db");
    std::cout << "Hello, World!" << std::endl;

    SerdeJsonMap map;
    map.init();

    nlohmann::json j2 = {
        {"pi", 3.141},
        {"happy", true},
        {"name", "Niels"},
        {"nothing", nullptr},
        {"answer", {{"everything", 42}}},
        {"list", {1, 0, 2}},
        {"object", {{"currency", "USD"}, {"value", 42.99}}}};

    map.from_json(j2);

  //  map.debug_print_cpp();

   // map.debug_print();

   // nlohmann::json j3;

   // map.to_json(j3);

   // std::cout << j3.dump(4) << std::endl;

    auto col = hoardbase_create_collection(db, "test");

    auto val = serde_json_map2value(map.m_internal);
    map.m_internal = nullptr;

    auto record = reinterpret_cast<Record*>(hoardbase_collection_insert_one(col, val));

    std::cout << "record id: " << record << std::endl;

    record->debug_print();

    delete record;
   // close(db);


}