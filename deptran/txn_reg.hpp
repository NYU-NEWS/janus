#pragma once

class TxnRegistry {
public:
    typedef std::function<void (
            const RequestHeader& header,
            const Value* input,
            rrr::i32 input_size,
            rrr::i32* res,
            Value* output,
            rrr::i32* output_size,
            row_map_t *row_map,
//            cell_entry_map_t *entry_map
            Vertex<PieInfo> *pv,
            Vertex<TxnInfo> *tv,
            std::vector<TxnInfo *> *ro_conflict_txns
            )> TxnHandler;

    typedef enum {
        DF_REAL,
        DF_NO,
        DF_FAKE
    } defer_t;

    typedef struct {
        TxnHandler txn_handler;
        defer_t defer;
    } txn_handler_defer_pair_t;

//    typedef std::function<void (const RequestHeader& header,
//                                const Value* input,
//                                rrr::i32 input_size,
//                                std::unordered_map<cell_locator_t, int, cell_locator_t_hash> *opset)> LockSetOracle;

    static inline void reg(
            base::i32 t_type, 
            base::i32 p_type,
            defer_t defer, 
            const TxnHandler& txn_handler) {
        auto func_key = std::make_pair(t_type, p_type);
        auto it = all_.find(func_key);
        verify(it == all_.end());
        all_[func_key] = (txn_handler_defer_pair_t){txn_handler, defer};
    }

//    static inline void reg_lock_oracle(base::i32 t_type, base::i32 p_type, const LockSetOracle& lck_oracle) {
//        auto func_key = std::make_pair(t_type, p_type);
//        auto it = lck_oracle_.find(func_key);
//        verify(it == lck_oracle_.end());
//        lck_oracle_[func_key] = lck_oracle;
//    }

    static inline txn_handler_defer_pair_t get(
            const base::i32 t_type,
            const base::i32 p_type) {
        auto it = all_.find(std::make_pair(t_type, p_type));
        // Log::debug("t_type: %d, p_type: %d", t_type, p_type);
        verify(it != all_.end());
        return it->second;
    }

    static inline txn_handler_defer_pair_t get(const RequestHeader& req_hdr) {
        return get(req_hdr.t_type, req_hdr.p_type);
    }

//    static inline LockSetOracle get_lock_oracle(base::i32 t_type, base::i32 p_type) {
//        auto it = lck_oracle_.find(std::make_pair(t_type, p_type));
//        // Log::debug("t_type: %d, p_type: %d", t_type, p_type);
//        verify(it != lck_oracle_.end());
//        verify(it->second != nullptr);
//        return it->second;
//    }
//
//    static inline LockSetOracle get_lock_oracle(const RequestHeader& req_hdr) {
//        return get_lock_oracle(req_hdr.t_type, req_hdr.p_type);
//    }

    static void pre_execute_2pl(const RequestHeader& header,
                               const std::vector<mdb::Value>& input,
                               rrr::i32* res,
                               std::vector<mdb::Value>* output,
                               DragonBall *db);

    static void pre_execute_2pl(const RequestHeader& header,
                               const Value *input,
                               rrr::i32 input_size,
                               rrr::i32* res,
                               mdb::Value* output,
                               rrr::i32* output_size,
                               DragonBall *db);

    static inline void execute(const RequestHeader& header,
                               const std::vector<mdb::Value>& input,
                               rrr::i32* res,
                               std::vector<mdb::Value>* output) {
        rrr::i32 output_size = output->size();
        get(header).txn_handler(header, input.data(), input.size(),
				res, output->data(), &output_size,
				NULL, NULL, NULL, NULL);
        output->resize(output_size);
    }

    static inline void execute(const RequestHeader& header,
                               const Value *input,
                               rrr::i32 input_size,
                               rrr::i32* res,
                               mdb::Value* output,
                               rrr::i32* output_size) {
        get(header).txn_handler(header, input, input_size,
				res, output, output_size,
				NULL, NULL, NULL, NULL);
    }

    static void exe_deptran_start(
            const RequestHeader &header,
            const std::vector<mdb::Value> &input,
            bool &is_defered,
            std::vector<mdb::Value> &output,
            Vertex<PieInfo> *pv,
            Vertex<TxnInfo> *tv
            //cell_entry_map_t *rw_entry
            );

    static void exe_deptran_start_ro(
            const RequestHeader &header,
            const std::vector<mdb::Value> &input,
            std::vector<mdb::Value> &output,
            std::vector<TxnInfo *> *conflict_txns
            );

    static void exe_deptran_finish(i64 tid,
        std::vector<std::pair<RequestHeader, std::vector<mdb::Value> > > &outputs);

    typedef struct {
        RequestHeader header;
        std::vector<mdb::Value> inputs;
        row_map_t row_map;
    } DeferredRequest;

private:
    // prevent instance creation
    TxnRegistry() {}
    static map<std::pair<base::i32, base::i32>, txn_handler_defer_pair_t> all_;
//    static map<std::pair<base::i32, base::i32>, LockSetOracle> lck_oracle_;
    static map<i64, std::vector<DeferredRequest>> deferred_reqs_;

};
