#pragma once

#include "deptran/__dep__.h"
#include "deptran/txn_chopper.h"

namespace deptran {

class Coordinator;

class RWChopper : public TxnCommand {
private:
    void W_txn_init(TxnRequest &req);
    void R_txn_init(TxnRequest &req);

public:
    RWChopper();

    virtual void Init(TxnRequest &req);

    virtual bool start_callback(const std::vector<int> &pi,
                                int res,
                                BatchStartArgsHelper &bsah);

    virtual bool start_callback(int pi,
                                int res,
                                map<int32_t, Value> &output);

    virtual bool IsReadOnly();

    virtual void Reset();

    virtual ~RWChopper();

};

}
