#include "model.hpp"
#include <iostream>
#include <limits>
using namespace certskip;
static void require(bool condition) {
    if (!condition) throw std::runtime_error("Native core test failed");
}
int main() {
    Interval broad{Value(int64_t(20)), Value(int64_t(30)), true, true};
    Interval point{Value(int64_t(25)), Value(int64_t(25)), true, true};
    require(covers({{"age", broad}}, {{"age", point}}));
    require(!covers({{"age", point}}, {{"age", broad}}));
    require(!covers({{"age", broad}}, {}));
    broad.lower_closed = false;
    require(!matches(broad, Value(int64_t(20))));
    require(compare(Value(int64_t(-1)), Value(std::numeric_limits<uint64_t>::max())) < 0);
    require(compare(Value(std::numeric_limits<uint64_t>::max()), Value(int64_t(0))) > 0);
    require(!same_type(Value(true), Value(int64_t(1))));
    auto chosen = select({1, 1, 1}, {{{0, 5}, {1, 5}}, {{1, 5}, {2, 1}}, {{3, 4}}}, 2);
    require(chosen == std::vector<size_t>({0, 2}));
    require(select({2}, {{{0, 10}}}, 1).empty());
    std::cout << "Native containment, integer boundaries and overlap selection passed\n";
}
