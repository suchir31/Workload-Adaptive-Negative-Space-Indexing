#include "model.hpp"
namespace certskip {
bool covers(const Region& certificate, const Region& query) {
    for (const auto& [name, outer] : certificate) {
        auto found = query.find(name);
        if (found == query.end()) return false;
        const auto& inner = found->second;
        const auto& a = outer.lower ? *outer.lower : *outer.upper;
        const auto& b = inner.lower ? *inner.lower : *inner.upper;
        if (!same_type(a, b)) return false;
        if (outer.lower) {
            if (!inner.lower) return false;
            int c = compare(*inner.lower, *outer.lower);
            if (c < 0 || (c == 0 && inner.lower_closed && !outer.lower_closed)) return false;
        }
        if (outer.upper) {
            if (!inner.upper) return false;
            int c = compare(*inner.upper, *outer.upper);
            if (c > 0 || (c == 0 && inner.upper_closed && !outer.upper_closed)) return false;
        }
    }
    return true;
}
}
