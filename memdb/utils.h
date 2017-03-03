#pragma once

#include <map>
#include <string>
#include <unordered_map>

#include "rrr.hpp"

namespace mdb {

using base::i32;
using base::i64;
using base::NoCopy;
using base::RefCounted;
using base::Enumerator;
using base::Log;
using base::insert_into_map;
using base::format_decimal;

typedef uint64_t version_t;
typedef int colid_t;

typedef enum {
    NONE,

    ROW_BASIC,
    ROW_COARSE,
    ROW_FINE,
    ROW_VERSIONED,
    ROW_MULTIVER,

    TBL_SORTED,
    TBL_UNSORTED,
    TBL_SNAPSHOT,

    TXN_UNSAFE,
    TXN_NESTED,
    TXN_2PL,
    TXN_OCC,

    TXN_ABORT,
    TXN_COMMIT,

    ORD_ANY,
    ORD_ASC,
    ORD_DESC,

    OCC_EAGER,
    OCC_LAZY,
} symbol_t;

uint32_t stringhash32(const void* data, int len);

inline uint32_t stringhash32(const std::string& str) {
    return stringhash32(&str[0], str.size());
}

uint64_t stringhash64(const void* data, int len);

inline uint64_t stringhash64(const std::string& str) {
    return stringhash64(&str[0], str.size());
}

uint32_t inthash32(uint32_t* key, int key_length);
uint64_t inthash64(uint64_t* key, int key_length);

inline uint32_t inthash32(uint32_t key1, uint32_t key2) {
    uint32_t arr[] = {key1, key2};
    return inthash32(arr, 2);
}

inline uint64_t inthash64(uint64_t key1, uint64_t key2) {
    uint64_t arr[] = {key1, key2};
    return inthash64(arr, 2);
}

} // namespace mdb
