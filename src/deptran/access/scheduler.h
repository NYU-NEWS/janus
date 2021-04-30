//
// Created by chance_Lv on 2020/2/10.
//
#pragma once

#include <deptran/classic/scheduler.h>
#include "commo.h"

namespace janus {
    class SchedulerAcc : public SchedulerClassic {
        using SchedulerClassic::SchedulerClassic;
    public:
        int32_t OnDispatch(cmdid_t cmd_id,
                           const shared_ptr<Marshallable>& cmd,
                           uint64_t ssid_spec,
                           uint64_t safe_ts,
                           uint8_t is_single_shard_write_only,
                           uint64_t* ssid_min,
                           uint64_t* ssid_max,
                           uint64_t* ssid_new,
                           TxnOutput& ret_output,
                           uint64_t* arrival_time,
                           uint8_t* rotxn_okay,
                           std::pair<parid_t, uint64_t>* new_svr_ts);     // AccDispatch RPC handler
        void OnValidate(cmdid_t cmd_id, snapshotid_t ssid_new, int8_t* res);
        void OnFinalize(cmdid_t cmd_id, int8_t decision);
        void OnStatusQuery(cmdid_t cmd_id, int8_t* res, DeferredReply* defer);
	    void check_status(cmdid_t cmd_id, bool& is_decided, bool& will_abort);
        bool Guard(Tx &tx_box, Row* row, int col_id, bool write) override {
            // do nothing, just has to override this pure virtual func.
            return true;
        }
        // for failure handling RPC calls
        AccCommo* commo();
        void OnResolveStatusCoord(cmdid_t cmd_id, uint8_t* status, DeferredReply* defer);
        void AccResolveStatusCoordAck(cmdid_t tid, uint8_t status);
        void OnGetRecord(cmdid_t cmd_id, uint8_t* status, uint64_t* ssid_low, uint64_t* ssid_high);
        void AccGetRecordAck(cmdid_t tid, uint8_t status, uint64_t ssid_low, uint64_t ssid_high);
        // for failure handling local call
        txn_status_t resolve_status(cmdid_t cmd_id);
        void handle_failure(cmdid_t cmd_id, parid_t pid);
        shared_ptr<Tx> GetAccTxn(txnid_t tid);
        // for rotxn
        i32 get_key(const shared_ptr<SimpleCommand>& c) const;

    private:
        const i32 NOT_ROW_KEY = -1;
        const uint64_t MAX_QUERY_TIMEOUT = 5000000; // maximum timeout for acc_query rpc is 5 seconds.
        const uint64_t MAX_BLOCK_TIMEOUT = 3000000; // max time a txn will be blocked
        i32 get_row_key(const shared_ptr<SimpleCommand>& sp_piece_data, int32_t var_id, uint8_t workload) const;
        bool tpcc_row_key(int32_t var_id) const;
        // rotxn
        static uint64_t max_write_ts;
    };
}
