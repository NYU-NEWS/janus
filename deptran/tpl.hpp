#pragma once


class TPLDTxn : public DTxn {
public:
    mdb::Txn* mdb_txn_;

    TPLDTxn(i64 tid, DTxnMgr* mgr);

    int prepare();

    int commit();

    int abort();

    // This method should not be used for now.
    mdb::Row* create(const mdb::Schema* schema, const std::vector<mdb::Value>& values) {
        verify(0);
        return nullptr;
    }

    std::function<void(void)> get_2pl_proceed_callback(
            const RequestHeader &header,
            const mdb::Value *input,
            rrr::i32 input_size,
            rrr::i32 *res
    );

    std::function<void(void)> get_2pl_fail_callback(
            const RequestHeader &header,
            rrr::i32 *res,
            mdb::Txn2PL::PieceStatus *ps
    );

    std::function<void(void)> get_2pl_succ_callback(
            const RequestHeader &header,
            const mdb::Value *input,
            rrr::i32 input_size,
            rrr::i32 *res,
            mdb::Txn2PL::PieceStatus *ps,
            std::function<void(
                    const RequestHeader &,
                    const Value *,
                    rrr::i32,
                    rrr::i32 *)> func
    );

    std::function<void(void)> get_2pl_succ_callback(
            const RequestHeader &req,
            const mdb::Value *input,
            rrr::i32 input_size,
            rrr::i32 *res,
            mdb::Txn2PL::PieceStatus *ps
    );

    // Below are merged from TxnRegistry.
    void pre_execute_2pl(
            const RequestHeader& header,
            const std::vector<mdb::Value>& input,
            rrr::i32* res,
            std::vector<mdb::Value>* output,
            DragonBall *db
    );


    void pre_execute_2pl(
            const RequestHeader& header,
            const Value *input,
            rrr::i32 input_size,
            rrr::i32* res,
            mdb::Value* output,
            rrr::i32* output_size,
            DragonBall *db
    );

    void execute(
            const RequestHeader& header,
            const std::vector<mdb::Value>& input,
            rrr::i32* res,
            std::vector<mdb::Value>* output
    ) {
        rrr::i32 output_size = output->size();
        TxnRegistry::get(header).txn_handler(nullptr, header, input.data(), input.size(),
                res, output->data(), &output_size,
                NULL);
        output->resize(output_size);
    }

    inline void execute(
            const RequestHeader& header,
            const Value *input,
            rrr::i32 input_size,
            rrr::i32* res,
            mdb::Value* output,
            rrr::i32* output_size
    ) {
        TxnRegistry::get(header).txn_handler(nullptr, header, input, input_size,
                res, output, output_size,
                NULL);
    }
};