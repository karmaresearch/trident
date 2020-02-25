#include <trident/utils/json.h>

int main(int argc, const char** argv) {
    std::string in = "{ \"a\": { \"c\": \"b\"}}";
    JSON value;
    JSON::read(in, value);
}
