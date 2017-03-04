#pragma once

#include "deptran/__dep__.h"
#include "deptran/procedure.h"

namespace deptran {

class Coordinator;

class RWChopper : public Procedure {
private:
    void W_txn_init(TxnRequest &req);
    void R_txn_init(TxnRequest &req);

public:
    RWChopper();

    virtual void Init(TxnRequest &req);

    virtual bool HandleOutput(int pi,
                              int res,
                              map<int32_t, Value> &output);

    virtual bool IsReadOnly();

    virtual void Reset();

    virtual ~RWChopper();

};

}
