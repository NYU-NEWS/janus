
#pragma once

#include <sstream>
#include <string>

#include "rrr.hpp"
#include "memdb/value.h"


namespace rrr {

rrr::Marshal& operator << (rrr::Marshal& m, const mdb::Value &value);

rrr::Marshal& operator >> (rrr::Marshal& m, mdb::Value &value);

} // namespace rrr

