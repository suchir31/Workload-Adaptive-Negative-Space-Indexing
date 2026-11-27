#include "reader.hpp"
namespace certskip {
static Value scalar_value(const arrow::Scalar& s) {
    switch (s.type->id()) {
#define SIGNED_CASE(ID, TYPE) case arrow::Type::ID: return int64_t(static_cast<const arrow::TYPE##Scalar&>(s).value);
        SIGNED_CASE(INT8, Int8) SIGNED_CASE(INT16, Int16)
        SIGNED_CASE(INT32, Int32) SIGNED_CASE(INT64, Int64)
#undef SIGNED_CASE
#define UNSIGNED_CASE(ID, TYPE) case arrow::Type::ID: return uint64_t(static_cast<const arrow::TYPE##Scalar&>(s).value);
        UNSIGNED_CASE(UINT8, UInt8) UNSIGNED_CASE(UINT16, UInt16)
        UNSIGNED_CASE(UINT32, UInt32) UNSIGNED_CASE(UINT64, UInt64)
#undef UNSIGNED_CASE
        case arrow::Type::BOOL: return static_cast<const arrow::BooleanScalar&>(s).value;
        case arrow::Type::STRING: return std::string(static_cast<const arrow::StringScalar&>(s).view());
        default: throw std::invalid_argument("Unsupported Arrow predicate type");
    }
}
std::vector<bool> verify_table(const arrow::Table& table, const std::vector<Region>& regions) {
    std::vector<bool> empty(regions.size(), true);
    for (size_t c = 0; c < regions.size(); ++c) {
        std::vector<std::pair<std::shared_ptr<arrow::ChunkedArray>, Interval>> terms;
        for (const auto& [name, interval] : regions[c]) {
            auto column = table.GetColumnByName(name);
            if (!column) throw std::invalid_argument("Unknown or ambiguous column: " + name);
            terms.emplace_back(column, interval);
        }
        for (int64_t row = 0; row < table.num_rows(); ++row) {
            bool match = true;
            for (const auto& [column, interval] : terms) {
                auto scalar = take(column->GetScalar(row));
                if (!scalar->is_valid || !matches(interval, scalar_value(*scalar))) {
                    match = false;
                    break;
                }
            }
            if (match) { empty[c] = false; break; }
        }
    }
    return empty;
}
}
