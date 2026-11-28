#include "reader.hpp"
#include <unistd.h>
#include <parquet/file_reader.h>
#include <set>
#include <algorithm>
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
    if (group < 0 || group >= reader_->num_row_groups())
        throw std::invalid_argument("Invalid row group");
    std::shared_ptr<arrow::Table> table;
    check(reader_->ReadRowGroup(group, &table));
    return table;
}
std::vector<bool> Reader::verify(int group, const std::vector<Region>& regions, int64_t batch_size) {
    if (batch_size <= 0) throw std::invalid_argument("Batch size must be positive");
    if (group < 0 || group >= reader_->num_row_groups())
        throw std::invalid_argument("Invalid row group");
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
    reader_->set_batch_size(batch_size);
    auto batches = take(reader_->GetRecordBatchReader(
        {group}, std::vector<int>(indices.begin(), indices.end())));
    std::vector<bool> empty(regions.size(), true);
    while (true) {
        auto batch = take(batches->Next());
        if (!batch) break;
        std::vector<Region> pending;
        std::vector<size_t> ids;
        for (size_t i = 0; i < regions.size(); ++i) if (empty[i]) {
            pending.push_back(regions[i]);
            ids.push_back(i);
        }
        auto absent = verify_batch(*batch, pending);
        for (size_t i = 0; i < ids.size(); ++i) empty[ids[i]] = absent[i];
        if (std::none_of(empty.begin(), empty.end(), [](bool v) { return v; })) break;
    }
    return empty;
}
}
