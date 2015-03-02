#ifndef MICRO_PIECE_H_
#define MICRO_PIECE_H_

namespace deptran {

class Piece;

extern char MICRO_BENCH_TABLE_A[];
extern char MICRO_BENCH_TABLE_B[];
extern char MICRO_BENCH_TABLE_C[];
extern char MICRO_BENCH_TABLE_D[];

#define MICRO_BENCH_R     1
#define MICRO_BENCH_R_NAME "MICRO_R"

#define MICRO_BENCH_R_0   10
#define MICRO_BENCH_R_1   11
#define MICRO_BENCH_R_2   12
#define MICRO_BENCH_R_3   13

#define MICRO_BENCH_W     2
#define MICRO_BENCH_W_NAME "MICRO_W"

#define MICRO_BENCH_W_0   20
#define MICRO_BENCH_W_1   21
#define MICRO_BENCH_W_2   22
#define MICRO_BENCH_W_3   23

class MicroBenchPiece : public Piece {

public:

    void reg_all();

    void reg_pieces();

    void reg_lock_oracles();
};

}

#endif // MICRO_PIECE_H_
