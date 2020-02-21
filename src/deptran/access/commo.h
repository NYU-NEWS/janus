//
// Created by chance_Lv on 2020/2/18.
//
#pragma once
#include "communicator.h"

namespace janus {
    class AccCommo : public Communicator {
    public:
        using Communicator::Communicator;
        void AccBroadcastDispatch(shared_ptr<vector<shared_ptr<SimpleCommand>>> sp_vec_piece,
                                  Coordinator *coo,
                                  const std::function<void(int res,
                                                      uint64_t ssid_low,
                                                      uint64_t ssid_high,
                                                      uint64_t ssid_highest,
                                                      TxnOutput &)> &callback);
        void AccBroadcastValidate(parid_t par_id,
                                  cmdid_t cmd_id,
                                  snapshotid_t ssid_new,
                                  const std::function<void(int8_t res)> &callback);
        void AccBroadcastFinalize(parid_t par_id, cmdid_t cmd_id, int8_t decision, snapshotid_t ssid_new);
    };
}

