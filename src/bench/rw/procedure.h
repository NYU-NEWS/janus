#pragma once

#include "deptran/__dep__.h"
#include "deptran/procedure.h"

namespace janus {

class Coordinator;

class RWChopper : public TxData {
private:
    void W_txn_init(TxRequest &req);
    void R_txn_init(TxRequest &req);

public:
    RWChopper();

    virtual void Init(TxRequest &req);

    virtual bool HandleOutput(int pi,
                              int res,
                              map<int32_t, Value> &output);

    virtual bool IsReadOnly();

    virtual void Reset();

    virtual ~RWChopper();

};

} // namespace janus
