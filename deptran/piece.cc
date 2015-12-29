#include <bench/simple/SimpleBenchPiece.h>
#include "all.h"

namespace deptran {

Piece *Piece::get_piece(int benchmark) {
  switch (benchmark) {
    case TPCA:
      return new TpcaPiece();
    case TPCC:
      return new TpccPiece();
    case TPCC_DIST_PART:
      return new TpccDistPiece();
    case TPCC_REAL_DIST_PART:
      return new TpccRealDistPiece();
    case RW_BENCHMARK:
      return new RWPiece();
    case MICRO_BENCH:
      return new MicroBenchPiece();
    case SIMPLE_BENCH:
      return new SimpleBenchPiece();
    default:
      verify(0);
      return NULL;
  }
}

}
