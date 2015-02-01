#include "all.h"

namespace rococo {

std::map<i64, std::vector<RCC::DeferredRequest>> RCC::deferred_reqs_;

DepGraph *RCC::dep_s = NULL;


void RCC::exe_deptran_finish(i64 tid,
        std::vector<std::pair<RequestHeader, std::vector<mdb::Value> > >
        &outputs) {
    auto dt_it = deferred_reqs_.find(tid);
    if (dt_it == deferred_reqs_.end()) {
        // this tid does not need deferred execution.
        //verify(0);
    } else {
        // delayed execution
        auto txn_id = dt_it->first;
        auto &reqs = dt_it->second;
        outputs.clear(); // FIXME does this help? seems it does, why?
        for (auto &req: reqs) {
            auto &header = req.header;
            auto &input = req.inputs;
            auto txn_handler_pair = TxnRegistry::get(header.t_type, header.p_type);
            verify(header.tid == txn_id);

            std::vector<Value> output;
            int output_size = 300;
            output.resize(output_size);
            int res;
            txn_handler_pair.txn_handler(header, input.data(), input.size(),
                    &res, output.data(), &output_size,
                    &req.row_map, NULL, NULL, NULL);
            if (header.p_type == TPCC_PAYMENT_4
                    && header.t_type == TPCC_PAYMENT)
                verify(output_size == 15);
            output.resize(output_size);

            // FIXME. what the fuck happens here?
            auto pp = std::make_pair(header, output);
            outputs.push_back(pp);
        }
        deferred_reqs_.erase(dt_it);
    }
}

void RCC::exe_deptran_start(
        const RequestHeader &header,
        const std::vector<mdb::Value> &input,
        bool &is_defered,
        std::vector<mdb::Value> &output,
        Vertex<PieInfo> *pv,
        Vertex<TxnInfo> *tv
        /*cell_entry_map_t *rw_entry*/) {
    verify(pv && tv);
    auto txn_handler_pair = TxnRegistry::get(header.t_type, header.p_type);

    switch (txn_handler_pair.defer) {
        case DF_NO:
        { // immediate
            int output_size = 300;
            output.resize(output_size);
            int res;
            txn_handler_pair.txn_handler(header, input.data(),
                    input.size(), &res, output.data(),
                    &output_size, NULL, pv,
                    tv/*rw_entry*/, NULL);
            output.resize(output_size);
            is_defered = false;
            break;
        }
        case DF_REAL:
        { // defer
            DeferredRequest dr;
            dr.header = header;
            dr.inputs = input;
            std::vector<DeferredRequest> &drs = deferred_reqs_[header.tid];
            if (drs.size() == 0) {
                drs.reserve(100); //XXX
            }
            drs.push_back(dr);
            txn_handler_pair.txn_handler(header, drs.back().inputs.data(),
                    drs.back().inputs.size(), NULL, NULL,
                    NULL, &drs.back().row_map, pv,
                    tv/*rw_entry*/, NULL);
            is_defered = true;
            break;
        }
        case DF_FAKE: //TODO
        {
            DeferredRequest dr;
            dr.header = header;
            dr.inputs = input;
            std::vector<DeferredRequest> &drs = deferred_reqs_[header.tid];
            if (drs.size() == 0) {
                drs.reserve(100); //XXX
            }
            drs.push_back(dr);
            int output_size = 300; //XXX
            output.resize(output_size);
            int res;
            txn_handler_pair.txn_handler(header, drs.back().inputs.data(),
                    drs.back().inputs.size(), &res,
                    output.data(), &output_size,
                    &drs.back().row_map, pv,
                    tv/*rw_entry*/, NULL);
            output.resize(output_size);
            is_defered = false;
            break;
        }
        default:
            verify(0);
    }
}


void RCC::exe_deptran_start_ro(
        const RequestHeader &header,
        const std::vector<mdb::Value> &input,
        std::vector<mdb::Value> &output,
        std::vector<TxnInfo *> *conflict_txns) {
    auto txn_handler_pair = TxnRegistry::get(header.t_type, header.p_type);
    int output_size = 300;
    output.resize(output_size);
    int res;
    txn_handler_pair.txn_handler(header, input.data(), input.size(), &res,
            output.data(), &output_size, NULL, NULL,
            NULL, conflict_txns/*rw_entry*/);
    output.resize(output_size);
}

} // namespace rcc
