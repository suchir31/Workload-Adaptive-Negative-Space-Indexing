#include "model.hpp"
#include <set>
#include <limits>
#include <cmath>
namespace certskip {
std::vector<size_t> select(const std::vector<size_t>& costs,
                          const std::vector<std::map<size_t, double>>& benefits,
                          size_t budget) {
    if (costs.size() != benefits.size()) throw std::invalid_argument("Mismatched candidate counts");
    for (size_t i = 0; i < costs.size(); ++i) {
        if (!costs[i]) throw std::invalid_argument("Certificate cost must be positive");
        for (auto [key, value] : benefits[i])
            if (!std::isfinite(value) || value < 0) throw std::invalid_argument("Invalid benefit");
    }
    std::set<size_t> covered;
    std::vector<bool> used(costs.size(), false);
    std::vector<size_t> result;
    while (true) {
        size_t best = costs.size();
        long double score = 0;
        for (size_t i = 0; i < costs.size(); ++i) {
            if (used[i] || costs[i] > budget) continue;
            long double gain = 0;
            for (auto [key, value] : benefits[i]) if (!covered.count(key)) gain += value;
            if (gain / costs[i] > score) { best = i; score = gain / costs[i]; }
        }
        if (best == costs.size()) break;
        used[best] = true;
        budget -= costs[best];
        result.push_back(best);
        for (auto [key, value] : benefits[best]) covered.insert(key);
    }
    return result;
}
}
