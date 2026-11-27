#include "reader.hpp"
#include <unistd.h>
#include <parquet/file_reader.h>
#include <set>
namespace certskip {
Reader::Reader(int fd) {
    int owned = dup(fd);
    if (owned < 0) throw std::runtime_error("Cannot duplicate snapshot descriptor");
    auto opened = arrow::io::ReadableFile::Open(owned);
    if (!opened.ok()) { close(owned); check(opened.status()); }
    file_ = take(std::move(opened));
    reader_ = take(parquet::arrow::OpenFile(file_, arrow::default_memory_pool()));
}
std::shared_ptr<arrow::Table> Reader::read(int group) {
    std::shared_ptr<arrow::Table> table;
    check(reader_->ReadRowGroup(group, &table));
    return table;
}
std::vector<bool> Reader::verify(int group, const std::vector<Region>& regions) {
    if (regions.empty()) return {};
    std::shared_ptr<arrow::Schema> schema;
    check(reader_->GetSchema(&schema));
    // Resolve top-level fields to Parquet leaf indices, including nested fields
    // preceding a supported primitive predicate column.
    const auto* parquet_schema = reader_->parquet_reader()->metadata()->schema();
    std::set<int> indices;
    for (const auto& region : regions) for (const auto& [name, interval] : region) {
        int field = schema->GetFieldIndex(name);
        if (field < 0) throw std::invalid_argument("Unknown or ambiguous column: " + name);
        for (int i = 0; i < parquet_schema->num_columns(); ++i)
            if (parquet_schema->GetColumnRoot(i)->name() == name) indices.insert(i);
    }
    if (indices.empty()) {
        bool empty = reader_->parquet_reader()->metadata()->RowGroup(group)->num_rows() == 0;
        return std::vector<bool>(regions.size(), empty);
    }
    std::shared_ptr<arrow::Table> table;
    check(reader_->ReadRowGroup(group, std::vector<int>(indices.begin(), indices.end()), &table));
    return verify_table(*table, regions);
}
}
