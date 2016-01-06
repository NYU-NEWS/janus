#ifndef RW_BENCHMARK_PIE_H_
#define RW_BENCHMARK_PIE_H_

#include "all.h"

namespace deptran {

extern char RW_BENCHMARK_TABLE[];

#define RW_BENCHMARK_W_TXN  1
#define RW_BENCHMARK_R_TXN  2
#define RW_BENCHMARK_W_TXN_NAME  "WRITE"
#define RW_BENCHMARK_R_TXN_NAME  "READ"

#define RW_BENCHMARK_W_TXN_0 10
#define RW_BENCHMARK_R_TXN_0 20

class RWPiece : public Piece {
public:
    void reg_all();
    void reg_pieces();
};

}

#endif
