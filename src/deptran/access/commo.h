//
// Created by chance_Lv on 2020/2/18.
//
#pragma once
#include "communicator.h"

namespace janus {
    class AccCommo : public Communicator {
    public:
        using Communicator::Communicator;
        void AccBroadcastDispatch(uint32_t coo_id,
                                  shared_ptr<vector<shared_ptr<SimpleCommand>>> sp_vec_piece,
                                  Coordinator *coo,
                                  snapshotid_t ssid_spec,
                                  bool is_single_shard_write_only,
                                  parid_t coord,
                                  const std::unordered_set<parid_t>& cohorts,
                                  const std::function<void(int res,
                                                      uint64_t ssid_low,
                                                      uint64_t ssid_high,
                                                      uint64_t ssid_new,
                                                      TxnOutput &,
                                                      uint64_t arrival_time)> &callback,
                                  cmdid_t cmd_id,
                                  int& n_status_query,
                                  const std::function<void(int8_t res)> &callback_status);
        void AccBroadcastValidate(parid_t par_id,
                                  cmdid_t cmd_id,
                                  snapshotid_t ssid_new,
                                  const std::function<void(int8_t res)> &callback);
        void AccBroadcastFinalize(parid_t par_id, cmdid_t cmd_id, int8_t decision);
	    void AccBroadcastFinalizeAbort(parid_t par_id, cmdid_t cmd_id, int8_t decision, const std::function<void()> &callback);
        void AccBroadcastStatusQuery(parid_t par_id, cmdid_t cmd_id, const std::function<void(int8_t res)> &callback);

        // for failure handling, called between servers
        void AccBroadcastResolveStatusCoord(parid_t coord,
                                            cmdid_t cmd_id,
                                            const std::function<void(uint8_t status)> &callback);   // cohort sent to coord
    };
}

