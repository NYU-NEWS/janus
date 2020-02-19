#include "commo.h"

namespace janus {
    void AccCommo::AccBroadcastDispatch(shared_ptr<vector<shared_ptr<SimpleCommand>>> sp_vec_piece,
                                        Coordinator *coo,
                                        const std::function<void(int res,
                                                                 uint64_t ssid_low,
                                                                 uint64_t ssid_high,
                                                                 uint64_t ssid_highest,
                                                                 TxnOutput &)> &callback) {
        cmdid_t cmd_id = sp_vec_piece->at(0)->root_id_;
        verify(sp_vec_piece->size() > 0);
        auto par_id = sp_vec_piece->at(0)->PartitionId();
        rrr::FutureAttr fuattr;
        fuattr.callback =
                [coo, this, callback](Future* fu) {
                    int32_t ret;
                    uint64_t ssid_low;
                    uint64_t ssid_high;
                    uint64_t ssid_highest;
                    TxnOutput outputs;
                    fu->get_reply() >> ret >> ssid_low >> ssid_high >> ssid_highest >> outputs;
                    callback(ret, ssid_low, ssid_high, ssid_highest, outputs);
                };
        auto pair_leader_proxy = LeaderProxyForPartition(par_id);
        Log_debug("send dispatch to site %ld",
                  pair_leader_proxy.first);
        auto proxy = pair_leader_proxy.second;
        shared_ptr<VecPieceData> sp_vpd(new VecPieceData);
        sp_vpd->sp_vec_piece_data_ = sp_vec_piece;
        MarshallDeputy md(sp_vpd); // ????
        auto future = proxy->async_AccDispatch(cmd_id, md, fuattr); // call Acc dispatch RPC
        Future::safe_release(future);
        // FIXME fix this, this cause occ and perhaps 2pl to fail
        for (auto& pair : rpc_par_proxies_[par_id]) {
            if (pair.first != pair_leader_proxy.first) {
                rrr::FutureAttr fu2;
                fu2.callback =
                        [coo, this, callback](Future* fu) {
                            int32_t ret;
                            uint64_t ssid_low;
                            uint64_t ssid_high;
                            uint64_t ssid_highest;
                            TxnOutput outputs;
                            fu->get_reply() >> ret >> ssid_low >> ssid_high >> ssid_highest >> outputs;
                            // do nothing
                        };
                Future::safe_release(pair.second->async_AccDispatch(cmd_id, md, fu2));
            }
        }
    }
}