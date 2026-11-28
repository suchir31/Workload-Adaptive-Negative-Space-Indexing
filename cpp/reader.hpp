#pragma once
#include "model.hpp"
#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>
namespace certskip {
inline void check(const arrow::Status& status) {
    if (!status.ok()) throw std::runtime_error(status.ToString());
}
template <class T> T take(arrow::Result<T> result) {
    check(result.status());
    return std::move(result).ValueOrDie();
}
class Reader {
    std::shared_ptr<arrow::io::ReadableFile> file_;
    std::unique_ptr<parquet::arrow::FileReader> reader_;
public:
    explicit Reader(int fd);
    std::shared_ptr<arrow::Table> read(int group);
    std::vector<bool> verify(int group, const std::vector<Region>& regions, int64_t batch_size = 65536);
};
std::vector<bool> verify_batch(const arrow::RecordBatch& batch, const std::vector<Region>& regions);
}
