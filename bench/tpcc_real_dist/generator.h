
#pragma once

#include "../tpcc/generator.h"

namespace rococo {

class TpccdTxnGenerator : public TpccTxnGenerator {
 public:
  TpccdTxnGenerator(rococo::Config *config);
};

} // namespace rococo;