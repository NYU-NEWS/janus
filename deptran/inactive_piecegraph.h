//
//#ifndef PIECE_GRAPH_H_
//#define PIECE_GRAPH_H_
//
//#include "all.h"
//
//namespace deptran {
//
//class PieceGraph {
//
//public:
//    std::mutex m_graph_lock;
//    std::map<std::string, std::map<std::string, std::vector<Piece*> > > dep_entry_;
//    std::map<uint64_t, Piece*> pie_entry_;
//    int WW = 0;
//    int WR = 0;
//    int RW = 0;
//
//    /**
//     * check whether the last piece is conflict with the ahead. On table tn and key k.
//     */
//    int checkPieceConflict(std::vector<Piece*> &vec, Piece* pie, std::string tn, std::string k) {
//        for (Piece *p : vec) {
//            if (p == pie) {
//                break;
//            }
//
//            if (p->status_ == START) {
//                int pie_type = pie->op_map_[tn][k];
//                int p_type = p->op_map_[tn][k];
//
//                if (pie_type & WRITE) {
//                    return 1;
//                }
//
//                if ( (pie_type & READ) && (p_type & WRITE) ) {
//                    return 1;
//                }
//            } else if (p->status_ == COMMIT || p->status_ == ABORT) {
//                continue;
//            }
//        }
//        return 0;
//    }
//
//    int pushPiece(Piece *pie) {
//        std::lock_guard<std::mutex> dep_lock(this->m_graph_lock);
//        //printf("push pie id: %" PRIx64 " into graph.\n", pie->pie_id_);
//
//        int conflict = 0;
//        for (auto &tn : pie->op_map_) {
//            for (auto &rk : tn.second) {
//                std::vector<Piece*> &vec = dep_entry_[tn.first][rk.first];
//                vec.push_back(pie);
//                conflict |= checkPieceConflict(vec, pie, tn.first, rk.first);
//                this->pie_entry_[pie->pie_id_] = pie;
//                //printf("push pie id: %" PRIx64 " into entry.\n", pie->pie_id_);
//            }
//        }
//
//        if (conflict) {
//            return CONTENTION;
//        } else {
//            return SUCCESS;
//        }
//    }
//
//    /**
//     * this piece is committed or aborted, check whether the next piece is available to wake up.
//     */
//    int checkNextPiece(Piece *pie) {
//        for (auto &tn : pie->op_map_) {
//            for (auto &rk : tn.second) {
//                std::vector<Piece*> &vec = dep_entry_[tn.first][rk.first];
//
//                int end = 0;
//                Piece *next_p = NULL;
//                for (Piece *p : vec) {
//                    if (end) {
//                        next_p = p;
//                        break;
//                    }
//                    if (p == pie) {
//                        end ++;
//                    }
//                }
//
//                if (next_p == NULL) {
//                    continue;
//                }
//
//                if (next_p->status_ != START) {
//                    assert(0);
//                }
//
//                int conflict = 0;
//                for (auto &next_tn : next_p->op_map_) {
//                    for (auto &next_rk : next_tn.second) {
//                        if ( (next_tn.first == tn.first) && (next_rk.first == rk.first)) {
//                            continue;
//                        }
//                        std::vector<Piece*> &next_vec = this->dep_entry_[next_tn.first][next_rk.first];
//                        conflict |= checkPieceConflict(next_vec, next_p, next_tn.first, next_rk.first);
//                    }
//                }
//
//                if (conflict == 0) {
//                    //wake up the condition variable
//                    next_p->pie_cond_.notify_one();
//                }
//            }
//        }
//        return 0;
//    }
//
//    int commitPiece(uint64_t pie_id) {
//        std::lock_guard<std::mutex> dep_lock(this->m_graph_lock);
//        Piece* pie = this->pie_entry_[pie_id];
//        //printf("retrieve pie id: %" PRIx64 "\n", pie_id);
//        assert(pie != NULL);
//        pie->status_ = COMMIT;
//        checkNextPiece(pie);
//        return COMMIT;
//    }
//
//    int abortPiece(uint64_t pie_id) {
//        std::lock_guard<std::mutex> dep_lock(this->m_graph_lock);
//        Piece* pie = this->pie_entry_[pie_id];
//        pie->status_ = ABORT;
//        checkNextPiece(pie);
//        return COMMIT;
//    }
//
//};
//
//}
//#endif // PIECE_GRAPH_H_
//
