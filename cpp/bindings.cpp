#include "reader.hpp"
#include <arrow/python/pyarrow.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
namespace py = pybind11;
using namespace certskip;
static std::optional<Value> value(py::handle item) {
    if (item.is_none()) return std::nullopt;
    if (PyBool_Check(item.ptr())) return Value(py::cast<bool>(item));
    if (PyLong_Check(item.ptr())) {
        int overflow = 0;
        auto v = PyLong_AsLongLongAndOverflow(item.ptr(), &overflow);
        if (PyErr_Occurred()) throw py::error_already_set();
        if (!overflow) return Value(int64_t(v));
        auto u = PyLong_AsUnsignedLongLong(item.ptr());
        if (PyErr_Occurred()) throw py::error_already_set();
        return Value(uint64_t(u));
    }
    if (PyUnicode_Check(item.ptr())) return Value(py::cast<std::string>(item));
    throw py::type_error("Unsupported scalar");
}
static Region region(py::dict input) {
    Region result;
    for (auto term : input) {
        auto bound = py::cast<py::dict>(term.second);
        Interval i{value(bound["lower"]), value(bound["upper"]),
                   py::cast<bool>(bound["lower_closed"]), py::cast<bool>(bound["upper_closed"])};
        if (!i.lower && !i.upper) throw py::value_error("Unbounded interval");
        if (i.lower && i.upper && (!same_type(*i.lower, *i.upper) ||
            compare(*i.lower, *i.upper) > 0 || (compare(*i.lower, *i.upper) == 0 &&
            (!i.lower_closed || !i.upper_closed)))) throw py::value_error("Invalid interval");
        const auto& endpoint = i.lower ? *i.lower : *i.upper;
        if (!integer(endpoint) && (!i.lower || !i.upper || compare(*i.lower, *i.upper) != 0 ||
            !i.lower_closed || !i.upper_closed)) throw py::value_error("Equality required");
        result.emplace(py::cast<std::string>(term.first), i);
    }
    return result;
}
PYBIND11_MODULE(_native, m) {
    if (arrow::py::import_pyarrow() < 0) throw py::error_already_set();
    m.def("covers", [](py::dict outer, py::dict inner) { return covers(region(outer), region(inner)); });
    m.def("select", &certskip::select, py::arg("costs"), py::arg("benefits"),
          py::arg("budget"), py::arg("account_overlap") = true);
    py::class_<Reader>(m, "Reader")
        .def(py::init<int>())
        .def("read_row_group", [](Reader& reader, int group) {
            std::shared_ptr<arrow::Table> table;
            { py::gil_scoped_release release; table = reader.read(group); }
            auto* wrapped = arrow::py::wrap_table(table);
            if (!wrapped) throw py::error_already_set();
            return py::reinterpret_steal<py::object>(wrapped);
        })
        .def("verify", [](Reader& reader, int group, const std::vector<py::dict>& inputs, int64_t batch_size) {
            std::vector<Region> regions;
            for (const auto& input : inputs) regions.push_back(region(input));
            py::gil_scoped_release release;
            return reader.verify(group, regions, batch_size);
        }, py::arg("group"), py::arg("regions"), py::arg("batch_size") = 65536);
}
