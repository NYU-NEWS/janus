#pragma once

#include "all.h"

namespace rcc {

class TxnChopperFactory {
public:
    static TxnChopper* gen_chopper(TxnRequest& req, int benchmark);

};

} // namespace rcc
