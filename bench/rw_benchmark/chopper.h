#pragma once

#include "all.h"

namespace deptran {

class Coordinator;

class RWChopper : public TxnChopper {
private:
    void W_txn_init(TxnRequest &req);

    void R_txn_init(TxnRequest &req);

public:
    RWChopper();

    virtual void init(TxnRequest& req);

    virtual bool start_callback(const std::vector<int> &pi,
                                int res,
                                BatchStartArgsHelper &bsah);

    virtual bool start_callback(int pi,
                                int res,
                                map<int32_t, Value> &output);

    virtual bool is_read_only();

    virtual void retry();

    virtual ~RWChopper();

};

}
