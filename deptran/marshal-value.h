
#pragma once

#include "__dep__.h"

namespace rrr {

Marshal& operator << (Marshal& m, const mdb::Value &value);

Marshal& operator >> (Marshal& m, mdb::Value &value);

} // namespace rrr

