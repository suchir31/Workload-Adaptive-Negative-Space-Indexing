#include "model.hpp"
#include <set>
#include <queue>
#include <cmath>
namespace certskip {
namespace {
struct Ranked {
    long double score;
    size_t id;
    bool operator<(const Ranked& other) const {
        return score != other.score ? score < other.score : id > other.id;
    }
};
}
std::vector<size_t> select(const std::vector<size_t>& costs,
                          const std::vector<std::map<size_t, double>>& benefits,
                          size_t budget, bool account_overlap) {
    if (costs.size() != benefits.size()) throw std::invalid_argument("Mismatched candidate counts");
    std::set<size_t> covered;
    auto rank = [&](size_t i) {
        long double gain = 0;
        for (auto [key, value] : benefits[i])
            if (!account_overlap || !covered.count(key)) gain += value;
        return Ranked{gain / costs[i], i};
    };
    std::priority_queue<Ranked> queue;
    for (size_t i = 0; i < costs.size(); ++i) {
        if (!costs[i]) throw std::invalid_argument("Certificate cost must be positive");
        for (auto [key, value] : benefits[i])
            if (!std::isfinite(value) || value < 0) throw std::invalid_argument("Invalid benefit");
        if (costs[i] <= budget) queue.push(rank(i));
    }
    std::vector<size_t> result;
    // Marginal gains only decrease. Cached scores are upper bounds, so lazily
    // recomputing the best entry preserves exact greedy order (including ties).
    while (!queue.empty()) {
        auto best = queue.top();
        queue.pop();
        if (costs[best.id] > budget) continue;
        auto current = rank(best.id);
        if (current.score <= 0) continue;
        if (!queue.empty() && current < queue.top()) {
            queue.push(current);
            continue;
        }
        budget -= costs[best.id];
        result.push_back(best.id);
        if (account_overlap)
            for (auto [key, value] : benefits[best.id]) covered.insert(key);
    }
    return result;
}
}
