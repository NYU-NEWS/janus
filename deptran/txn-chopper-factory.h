#pragma once

#include "all.h"

namespace rococo {

class TxnChopperFactory {
public:
    static TxnChopper* gen_chopper(TxnRequest& req, int benchmark);

};

} // namespace rcc
