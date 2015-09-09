
#include "rrr.hpp"

#include "memdb/value.h"
#include "deptran/marshal-value.h"

using namespace base;
using namespace mdb;

TEST(marshal, value) {
    std::string s("Hello");
    Value v1((i32)1);
    Value v2((i64)2);
    Value v3((double)3.14);
    Value v4(s);
    Marshal m;

    Value v5, v6, v7, v8;
    m << v1 << v2 << v3 << v4;
    m >> v5 >> v6 >> v7 >> v8;

    EXPECT_EQ(v5.get_i32(), (int32_t)1);

    EXPECT_EQ(v6.get_i64(), (int64_t)2);

    EXPECT_EQ(v7.get_double(), (double)3.14);

    EXPECT_EQ(v8.get_str(), s);
}
