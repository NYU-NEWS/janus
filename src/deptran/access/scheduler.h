//
// Created by chance_Lv on 2020/2/10.
//
#pragma once

#include <deptran/classic/scheduler.h>

namespace janus {
    class SchedulerAcc : public SchedulerClassic {
        using SchedulerClassic::SchedulerClassic;
    public:
        int32_t OnDispatch(cmdid_t cmd_id,
                           const shared_ptr<Marshallable>& cmd,
                           uint64_t ssid_spec,
                           uint64_t* ssid_min,
                           uint64_t* ssid_max,
                           uint64_t* ssid_new,
                           TxnOutput& ret_output);     // AccDispatch RPC handler
        void OnValidate(cmdid_t cmd_id, snapshotid_t ssid_new, int8_t* res);
        void OnFinalize(cmdid_t cmd_id, int8_t decision);
        void OnStatusQuery(cmdid_t cmd_id, int8_t* res, DeferredReply* defer);
	    void check_status(cmdid_t cmd_id, bool& is_decided, bool& will_abort);
        bool Guard(Tx &tx_box, Row* row, int col_id, bool write) override {
            // do nothing, just has to override this pure virtual func.
            return true;
        }
    private:
        const i32 NOT_ROW_KEY = -1;
        i32 get_row_key(const shared_ptr<SimpleCommand>& sp_piece_data, int32_t var_id, uint8_t workload) const;
        bool tpcc_row_key(int32_t var_id) const;
    };
}
