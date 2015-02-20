#pragma once

#include <sstream>

#include "base/all.hpp"
#include "memdb/row.h"
#include "memdb/schema.h"
#include "memdb/txn.h"


template <class T>
void report_qps(const char* action, T n_ops, double duration) {
    base::Log::info("%s: %d ops, took %.2lf sec, qps=%s",
        action, n_ops, duration, base::format_decimal(T(n_ops / duration)).c_str());
}


template <class EnumeratorOfRows>
bool rows_are_sorted(EnumeratorOfRows rows, mdb::symbol_t order = mdb::symbol_t::ORD_ASC) {
    if (order == mdb::symbol_t::ORD_ANY) {
        return true;
    }
    verify(order == mdb::symbol_t::ORD_ASC || order == mdb::symbol_t::ORD_DESC);
    if (!rows.has_next()) {
        return true;
    }
    const mdb::Row* last = rows.next();
    while (rows.has_next()) {
        const mdb::Row* new_one = rows.next();
        if (order == mdb::symbol_t::ORD_DESC) {
            if (*last < *new_one) {
                return false;
            }
        } else {
            if (*last > *new_one) {
                return false;
            }
        }
    }
    return true;
}


static inline void print_row(const mdb::Row* r) {
    const mdb::Schema* sch = r->schema();
    std::ostringstream ostr;
    for (auto& col : *sch) {
        ostr << " " << r->get_column(col.id);
    }
    base::Log::info("row:%s", ostr.str().c_str());
}

static inline void print_row(mdb::Txn* txn, mdb::Row* r) {
    const mdb::Schema* sch = r->schema();
    std::ostringstream ostr;
    for (auto& col : *sch) {
        mdb::Value v;
        if (txn->read_column(r, col.id, &v)) {
            ostr << " " << v;
        } else {
            ostr << " (read failure)";
        }
    }
    base::Log::info("row:%s", ostr.str().c_str());
}

template <class EnumeratorOfRows>
void print_result(EnumeratorOfRows rows) {
    while (rows) {
        const mdb::Row* r = rows.next();
        print_row(r);
    }
}

template <class EnumeratorOfRows>
void print_result(mdb::Txn* txn, EnumeratorOfRows rows) {
    while (rows) {
        mdb::Row* r = rows.next();
        print_row(txn, r);
    }
}


template <class GenericTable>
inline void print_table(GenericTable* tbl) {
    print_result(tbl->all());
}

template <class GenericTable>
inline void print_table(mdb::Txn* txn, GenericTable* tbl) {
    print_result(txn, txn->all(tbl));
}


template <class GenericEnumerator>
inline int enumerator_count(GenericEnumerator e) {
    int size = 0;
    while (e) {
        e.next();
        size++;
    }
    return size;
}
