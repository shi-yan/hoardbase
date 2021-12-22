#include "serde_json.h"
#include <hoardbase.h>
#include <iostream>

SerdeJsonValue::~SerdeJsonValue()
{
}

void SerdeJsonValue::debug_print()
{
    if (m_internal)
    {
        serde_json_value_debug_print(m_internal);
    }
}

bool SerdeJsonMap::init()
{
    this->m_internal = serde_json_map_new();

    if (this->m_internal)
    {
        return true;
    }
    return false;
}

SerdeJsonMap::~SerdeJsonMap()
{
    if (this->m_internal)
    {
        serde_json_map_drop(this->m_internal);
        this->m_internal = nullptr;
    }
}

bool SerdeJsonMap::insert(const char *key, SerdeJsonValue &value)
{
    if (this->m_internal)
    {
        auto new_map = serde_json_map_insert(this->m_internal, key, value.m_internal);
        // once inserted into the map, the value is owned by the map
        value.m_internal = nullptr;
        return new_map != nullptr;
    }
    return false;
}

bool SerdeJsonMap::insert(const char *key, int64_t value)
{
    if (this->m_internal)
    {
        return serde_json_map_insert(this->m_internal, key, serde_json_i642value(value)) != nullptr;
    }
    return false;
}

bool SerdeJsonMap::insert(const char *key, double value)
{
    if (this->m_internal)
    {
        return serde_json_map_insert(this->m_internal, key, serde_json_f642value(value)) != nullptr;
    }
    return false;
}

bool SerdeJsonMap::insert(const char *key, SerdeJsonVec &value)
{
    if (this->m_internal)
    {
        auto new_map = serde_json_map_insert(this->m_internal, key, value.m_internal);
        // once inserted into the map, the value is owned by the map
        value.m_internal = nullptr;
        return new_map != nullptr;
    }
    return false;
}

bool SerdeJsonMap::insert(const char *key, const char *value)
{
    if (this->m_internal)
    {
        return serde_json_map_insert(this->m_internal, key, serde_json_str2value(value)) != nullptr;
    }
    return false;
}

bool SerdeJsonMap::insert(const char *key, SerdeJsonMap &value)
{
    if (this->m_internal)
    {
        auto new_map = serde_json_map_insert(this->m_internal, key, value.m_internal);
        // once inserted into the map, the value is owned by the map
        value.m_internal = nullptr;
        return new_map != nullptr;
    }
    return false;
}

void SerdeJsonMap::debug_print()
{
    if (this->m_internal)
    {
        serde_json_map_debug_print(this->m_internal);
    }
}

bool SerdeJsonMap::from_json(const nlohmann::json &json)
{
    for (auto &[key, value] : json.items())
    {
        if (value.is_null())
        {
            serde_json_map_insert(this->m_internal, key.c_str(), serde_json_null2value());
        }
        else if (value.is_boolean())
        {
            serde_json_map_insert(this->m_internal, key.c_str(), serde_json_bool2value(value.get<bool>()));
        }
        else if (value.is_number_integer())
        {
            serde_json_map_insert(this->m_internal, key.c_str(), serde_json_i642value(value.get<int64_t>()));
        }
        else if (value.is_number_float())
        {
            serde_json_map_insert(this->m_internal, key.c_str(), serde_json_f642value(value.get<double>()));
        }
        else if (value.is_string())
        {
            serde_json_map_insert(this->m_internal, key.c_str(), serde_json_str2value(value.get<std::string>().c_str()));
        }
        else if (value.is_array())
        {
            SerdeJsonVec vec;
            vec.init();

            vec.from_json(value);

            serde_json_map_insert(this->m_internal, key.c_str(), serde_json_vec2value(vec.m_internal));
            // once inserted into the vec, the value is owned by the vec
            vec.m_internal = nullptr;
        }
        else if (value.is_object())
        {
            SerdeJsonMap map;
            map.init();

            map.from_json(value);

            serde_json_map_insert(this->m_internal, key.c_str(), serde_json_map2value(map.m_internal));

            // once inserted into the map, the value is owned by the map
            map.m_internal = nullptr;
        }
        else
        {
            std::cout << "unknown type" << std::endl;
            return false;
        }
    }
    return true;
}

bool SerdeJsonMap::to_json(nlohmann::json &json) {
    /*auto iter = serde_json_map_iter(this->m_internal);

    //for(int i = 0; i< count; ++i) {}
    auto item = serde_json_map_iter_next(iter);
    while(item) {
        serde_json_map_item_print(item);
        item = serde_json_map_iter_next(iter);
    }*/
}

bool SerdeJsonMap::debug_print_cpp()
{
    call_cpp_test();
}

bool SerdeJsonVec::init()
{
    this->m_internal = serde_json_vec_new();

    if (this->m_internal)
    {
        return true;
    }
    return false;
}

SerdeJsonVec::~SerdeJsonVec()
{
    if (this->m_internal)
    {
        serde_json_vec_drop(this->m_internal);
        this->m_internal = nullptr;
    }
}

void SerdeJsonVec::debug_print()
{
    if (this->m_internal)
    {
        serde_json_vec_debug_print(this->m_internal);
    }
}

bool SerdeJsonVec::from_json(const nlohmann::json &json)
{
    for (auto &element : json)
    {
        if (element.is_null())
        {
            serde_json_vec_push(this->m_internal, serde_json_null2value());
        }
        else if (element.is_boolean())
        {
            serde_json_vec_push(this->m_internal, serde_json_bool2value(element.get<bool>()));
        }
        else if (element.is_number_integer())
        {
            serde_json_vec_push(this->m_internal, serde_json_i642value(element.get<int64_t>()));
        }
        else if (element.is_number_float())
        {
            serde_json_vec_push(this->m_internal, serde_json_f642value(element.get<double>()));
        }
        else if (element.is_string())
        {
            serde_json_vec_push(this->m_internal, serde_json_str2value(element.get<std::string>().c_str()));
        }
        else if (element.is_array())
        {
            SerdeJsonVec vec;
            vec.init();

            serde_json_vec_push(this->m_internal, serde_json_vec2value(vec.m_internal));
            // once inserted into the vec, the value is owned by the vec
            vec.m_internal = nullptr;
        }
        else if (element.is_object())
        {
            SerdeJsonMap map;
            map.init();

            map.from_json(element);

            serde_json_vec_push(this->m_internal,  serde_json_map2value(map.m_internal));

            // once inserted into the map, the value is owned by the map
            map.m_internal = nullptr;
        }
        else
        {
            std::cout << "unknown type" << std::endl;
            return false;
        }
    }
    return true;
}