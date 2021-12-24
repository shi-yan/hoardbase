    
    #include <cstdint>
    #include <nlohmann/json.hpp>
    #include <chrono>
    
    class Record {
    public:
        int64_t id;
        nlohmann::json data;
        std::string hash;
         std::chrono::time_point<std::chrono::system_clock> last_modified;

         void debug_print();
    };