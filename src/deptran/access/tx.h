//
// Created by chance_Lv on 2020/2/5.
//
#pragma once

#include <memdb/value.h>
#include <deptran/tx.h>
#include "constants.h"
#include "row.h"
#include "safeguard.h"

namespace janus {
    class AccTxn : public Tx {
        using Tx::Tx;
    public:
        /* leave it as is for now, should pass in a const Value*& to avoid copy */
        bool ReadColumn(Row *row,
                        colid_t col_id,
                        Value* value,
                        int hint_flag) override;

        bool ReadColumns(Row *row,
                         const std::vector<colid_t>& col_ids,
                         std::vector<Value>* values,
                         int hint_flag) override;

        // hides the WriteColumn virtual function in Tx
        bool WriteColumn(Row *row,
                         colid_t col_id,
                         Value& value,
                         int hint_flag) override;

        // hides the WriteColumns virtual function in Tx
        bool WriteColumns(Row *row,
                          const std::vector<colid_t> &col_ids,
                          std::vector<Value>& values,
                          int hint_flag) override;

        //bool InsertRow(Table *tbl, Row *row) override;

	    int n_query_inc();
        int n_callback_inc();
        int n_callback() const {
            return sg._n_query_callback;
        }
        int n_query() const {
            return sg._n_query;
        }
        bool all_callbacks_received() const;
        void set_query_abort();
        int8_t query_result() const;
        bool is_query_done() const;
	    bool is_status_abort() const;
        void insert_callbacks(const shared_ptr<AccTxn>& acc_txn, int8_t *res, DeferredReply* defer, int rpc_id);

        ~AccTxn() override;
    private:
        SafeGuard sg;
	    std::vector<std::function<void(int8_t)>> query_callbacks;  // for AccStatusQuery RPC
	    std::unordered_map<int, int> subrpc_count;   // tracks the number of sub-callbacks for each query rpc
        std::unordered_map<int, int8_t> subrpc_status;
        void load_speculative_ssid(snapshotid_t ssid);
        snapshotid_t get_spec_ssid() const;
        shared_ptr<IntEvent> acc_query_start = Reactor::CreateSpEvent<IntEvent>();
        static const mdb::Value DUMMY_VALUE_I32;
        static const mdb::Value DUMMY_VALUE_I64;
        static const mdb::Value DUMMY_VALUE_DOUBLE;
        static const mdb::Value DUMMY_VALUE_STR;
        static void get_dummy_value(mdb::Value* value);

        friend class SchedulerAcc;
    };
}   // namespace janus

