#pragma once

#include "__dep__.h"

namespace rococo {

class TxnChopperFactory {
public:
    static TxnChopper* gen_chopper(TxnRequest& req, int benchmark);

};

} // namespace rcc
