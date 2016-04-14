#ifndef RW_BENCHMARK_PIE_H_
#define RW_BENCHMARK_PIE_H_

#include "deptran/piece.h"

namespace deptran {

extern char RW_BENCHMARK_TABLE[];

#define RW_BENCHMARK_W_TXN  (100)
#define RW_BENCHMARK_R_TXN  (200)
#define RW_BENCHMARK_W_TXN_NAME  "WRITE"
#define RW_BENCHMARK_R_TXN_NAME  "READ"

#define RW_BENCHMARK_W_TXN_0 (101)
#define RW_BENCHMARK_R_TXN_0 (201)

class RWPiece : public Piece {
public:
    void reg_all();
    void reg_pieces();
};

}

#endif
