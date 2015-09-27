
#pragma once

#include "__dep__.h"

namespace rrr {

rrr::Marshal& operator << (rrr::Marshal& m, const mdb::Value &value);

rrr::Marshal& operator >> (rrr::Marshal& m, mdb::Value &value);

} // namespace rrr

