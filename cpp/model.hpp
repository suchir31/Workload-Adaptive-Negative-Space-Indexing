#pragma once
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <variant>
#include <vector>
#include <stdexcept>

namespace certskip {
using Value = std::variant<int64_t, uint64_t, bool, std::string>;
struct Interval {
    std::optional<Value> lower, upper;
    bool lower_closed = true, upper_closed = true;
};
using Region = std::map<std::string, Interval>;
inline bool integer(const Value& v) { return v.index() < 2; }
inline bool same_type(const Value& a, const Value& b) {
    return (integer(a) && integer(b)) || a.index() == b.index();
}
inline int compare(const Value& a, const Value& b) {
    if (!same_type(a, b)) throw std::invalid_argument("Incompatible scalar types");
    if (integer(a)) {
        bool an = a.index() == 0 && std::get<int64_t>(a) < 0;
        bool bn = b.index() == 0 && std::get<int64_t>(b) < 0;
        if (an != bn) return an ? -1 : 1;
        if (an) {
            auto x = std::get<int64_t>(a), y = std::get<int64_t>(b);
            return (x > y) - (x < y);
        }
        auto x = a.index() == 0 ? uint64_t(std::get<int64_t>(a)) : std::get<uint64_t>(a);
        auto y = b.index() == 0 ? uint64_t(std::get<int64_t>(b)) : std::get<uint64_t>(b);
        return (x > y) - (x < y);
    }
    if (a.index() == 2) return int(std::get<bool>(a)) - int(std::get<bool>(b));
    return std::get<std::string>(a).compare(std::get<std::string>(b));
}
inline bool matches(const Interval& i, const Value& v) {
    if (i.lower && (!same_type(*i.lower, v) || compare(v, *i.lower) < 0 ||
        (!i.lower_closed && compare(v, *i.lower) == 0))) return false;
    if (i.upper && (!same_type(*i.upper, v) || compare(v, *i.upper) > 0 ||
        (!i.upper_closed && compare(v, *i.upper) == 0))) return false;
    return true;
}
bool covers(const Region& certificate, const Region& query);
std::vector<size_t> select(const std::vector<size_t>& costs,
                          const std::vector<std::map<size_t, double>>& benefits,
                          size_t budget);
}
