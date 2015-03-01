#include "all.h"

namespace rococo {

//int TPL::do_abort(i64 txn_id, rrr::DeferredReply* defer) {
//    //map<i64, txn_entry_t *>::iterator it = txn_map_s.find(txn_id);
//    //verify(it != txn_map_s.end() && it->second != NULL);
//    if (running_mode_s == MODE_2PL) {
//        //pthread_mutex_lock(&txn_map_mutex_s);
//        txn_entry_t *txn_entry = NULL;
//        map<i64, txn_entry_t *>::iterator it = txn_map_s.find(txn_id);
//        txn_entry = it->second;
//        //pthread_mutex_unlock(&txn_map_mutex_s);
//        txn_entry->abort_2pl(txn_id, &txn_map_s, /*&txn_map_mutex_s, */defer);
//    }
//    else {
//        map<i64, txn_entry_t *>::iterator it = txn_map_s.find(txn_id);
//        it->second->txn->abort();
//        delete it->second;
//        txn_map_s.erase(it);
//    }
//    return SUCCESS;
//}
//
//

int TPL::do_prepare(i64 txn_id) {
    auto txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(txn_id);
    verify(txn != NULL);
    switch (DTxnMgr::get_sole_mgr()->get_mode()) {
        case MODE_OCC:
            if (((mdb::TxnOCC *)txn)->commit_prepare())
                return SUCCESS;
            else
                return REJECT;
        case MODE_2PL: //XXX do logging
            if (((mdb::Txn2PL *)txn)->commit_prepare())
                return SUCCESS;
            else
                return REJECT;
        default:
            verify(0);
    }
}
int TPL::do_abort(i64 txn_id) {
    auto txn = DTxnMgr::get_sole_mgr()->del_mdb_txn(txn_id);
    verify(txn != NULL);
    txn->abort();
    delete txn;
    return SUCCESS;
}


int TPL::do_commit(i64 txn_id) {
    auto txn = DTxnMgr::get_sole_mgr()->del_mdb_txn(txn_id);
    verify(txn != NULL);
    switch (DTxnMgr::get_sole_mgr()->get_mode()) {
        case MODE_OCC:
            ((mdb::TxnOCC *)txn)->commit_confirm();
            break;
            //case MODE_DEPTRAN:
        case MODE_2PL:
            txn->commit();
            break;
        default:
            verify(0);
    }
    delete txn;
    return SUCCESS;
}

std::function<void(void)> TPL::get_2pl_succ_callback(
        const RequestHeader &header,
        const mdb::Value *input,
        rrr::i32 input_size,
        rrr::i32 *res,
        mdb::Txn2PL::PieceStatus *ps) {
    return [header, input, input_size, res, ps] () {
        Log::debug("tid: %ld, pid: %ld, p_type: %d, lock acquired call back",
                header.tid, header.pid, header.p_type);
        Log::debug("succ 1 callback: PS: %p", ps);
        verify(ps != NULL); //FIXME
        ps->start_yes_callback();
        Log::debug("tid: %ld, pid: %ld, p_type: %d, get lock",
                header.tid, header.pid, header.p_type);

        if (ps->can_proceed()) {
            Log::debug("proceed");
            if (ps->is_rejected()) {
                *res = REJECT;

                ps->remove_output();

                Log::debug("rejected");
            }
            else {
                std::vector<mdb::Value> *output_vec;
                mdb::Value *output;
                rrr::i32 *output_size;

                ps->get_output(&output_vec, &output, &output_size);

                if (output_vec != NULL) {
                    rrr::i32 output_vec_size = output_vec->size();
                    TxnRegistry::get(header).txn_handler(nullptr, header, input,
                            input_size, res, output_vec->data(),
                            &output_vec_size, NULL, NULL, NULL, NULL);
                    output_vec->resize(output_vec_size);
                }
                else {
                    TxnRegistry::get(header).txn_handler(nullptr, header, input,
                            input_size, res, output, output_size, NULL, NULL,
                            NULL, NULL);
                }
            }

            ps->trigger_reply_dragonball();
            ps->set_finish();
        }

    };
}

std::function<void(void)> TPL::get_2pl_proceed_callback(
        const RequestHeader &header,
        const mdb::Value *input,
        rrr::i32 input_size,
        rrr::i32 *res) {
    return [header, input, input_size, res] () {
        Log::debug("tid: %ld, pid: %ld, p_type: %d, no lock",
                header.tid, header.pid, header.p_type);
        mdb::Txn2PL::PieceStatus *ps = ((mdb::Txn2PL *)DTxnMgr::get_sole_mgr()->get_mdb_txn(header))
                ->get_piece_status(header.pid);
        verify(ps != NULL); //FIXME

        if (ps->is_rejected()) {
            *res = REJECT;

            ps->remove_output();

            Log::debug("rejected");
        }
        else {
            std::vector<mdb::Value> *output_vec;
            mdb::Value *output;
            rrr::i32 *output_size;

            ps->get_output(&output_vec, &output, &output_size);

            if (output_vec != NULL) {
                rrr::i32 output_vec_size = output_vec->size();
                TxnRegistry::get(header).txn_handler(nullptr, header, input,
                        input_size, res, output_vec->data(),
                        &output_vec_size, NULL, NULL, NULL, NULL);
                output_vec->resize(output_vec_size);
            }
            else {
                TxnRegistry::get(header).txn_handler(nullptr, header, input,
                        input_size, res, output, output_size, NULL, NULL,
                        NULL, NULL);
            }
        }

        ps->trigger_reply_dragonball();
        ps->set_finish();
    };
}

std::function<void(void)> TPL::get_2pl_fail_callback(
        const RequestHeader &header,
        rrr::i32 *res,
        mdb::Txn2PL::PieceStatus *ps) {
    return [header, res, ps] () {
        Log::debug("tid: %ld, pid: %ld, p_type: %d, lock timeout call back",
                header.tid, header.pid, header.p_type);
        //PieceStatus *ps = TPL::get_piece_status(header);
        Log::debug("fail callback: PS: %p", ps);
        verify(ps != NULL); //FIXME
        ps->start_no_callback();

        if (ps->can_proceed()) {
            *res = REJECT;

            ps->remove_output();

            ps->trigger_reply_dragonball();
            ps->set_finish();
        }
    };
}

std::function<void(void)> TPL::get_2pl_succ_callback(
        const RequestHeader &header,
        const mdb::Value *input,
        rrr::i32 input_size,
        rrr::i32 *res,
        mdb::Txn2PL::PieceStatus *ps,
        std::function<void(
                const RequestHeader &,
                const Value *,
                rrr::i32,
                rrr::i32 *)> func) {
    return [header, input, input_size, res, func, ps] () {
        Log::debug("tid: %ld, pid: %ld, p_type: %d, lock acquired call back",
                header.tid, header.pid, header.p_type);
        //PieceStatus *ps = TPL::get_piece_status(header);
        verify(ps != NULL); //FIXME
        Log::debug("succ 2 callback: PS: %p", ps);
        ps->start_yes_callback();
        Log::debug("tid: %ld, pid: %ld, p_type: %d, get lock",
                header.tid, header.pid, header.p_type);

        if (ps->can_proceed()) {
            Log::debug("proceed");
            if (ps->is_rejected()) {
                *res = REJECT;

                ps->remove_output();

                Log::debug("rejected");

                ps->trigger_reply_dragonball();
                ps->set_finish();
            }
            else {
                Log::debug("before func");
                func(header, input, input_size, res);
                Log::debug("end func");
            }
        }
    };
}



} // namespace rococo
