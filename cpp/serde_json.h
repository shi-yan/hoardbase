#include <cstdint> 
#include <nlohmann/json.hpp>

class SerdeJsonVec;

class SerdeJsonValue
{
    friend class SerdeJsonMap;
public:
    void *m_internal;

public:
    SerdeJsonValue() : m_internal(nullptr) {}
    virtual bool init() = 0;

    virtual ~SerdeJsonValue();

    virtual void debug_print();
};

class SerdeJsonMap : public SerdeJsonValue
{
    friend class SerdeJsonVec;
public:
    SerdeJsonMap() : SerdeJsonValue() {}
    bool init() override;

    ~SerdeJsonMap();

    bool insert(const char *key, SerdeJsonValue &value);
    bool insert(const char *key, int64_t value);
    bool insert(const char *key, double value);
    bool insert(const char *key, SerdeJsonVec &value);
    bool insert(const char *key, const char *value);
    bool insert(const char *key, SerdeJsonMap &value);

    void debug_print() override;

    bool from_json(const nlohmann::json &json);

    bool to_json(nlohmann::json &json);

    bool debug_print_cpp();
};

class SerdeJsonVec : public SerdeJsonValue
{
    friend class SerdeJsonMap;
public:
    SerdeJsonVec() : SerdeJsonValue() {}
    bool init() override;
    ~SerdeJsonVec();
    void debug_print() override;

    bool from_json(const nlohmann::json &json);
};