#include "__dep__.h"
#include "frame.h"
#include "config.h"
#include "bench/tpcc_real_dist/sharding.h"

namespace rococo {

Sharding* Frame::CreateSharding() {

  Sharding* ret;
  auto bench = Config::config_s->benchmark_;
  switch (bench) {
    case TPCC_REAL_DIST_PART:
      ret = new TPCCDSharding();
      break;
    default:
      verify(0);
  }

  return ret;
}


} // namespace rococo;