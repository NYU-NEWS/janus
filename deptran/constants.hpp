#ifndef CONSTANTS_HPP_
#define CONSTANTS_HPP_

namespace rcc {

/** read and write type */
#define WRITE   (0x01)
#define READ    (0x02)
#define REREAD  (0x04)

/** transaction type */
#define TXN_UNKNOWN (0x00)
//#define TXN_START   (0x01)
//#define TXN_FINISH  (0x02)
//#define TXN_COMMIT  (0x04)
#define TXN_ABORT   (0x08)

#define TXN_UKN (0x00)  // unknown
#define TXN_STD (0x01)  // started
#define TXN_CMT (0x02)  // committing
#define TXN_DCD (0x04)  // decided
#define TXN_FNS (0x08)  // finished
#define TXN_ABT (0x10)  // aborted

#define SUCCESS     (0)
#define CONTENTION  (-1)
#define REJECT      (-1)
#define DELAYED     (1)

#define MODE_NONE   (0)
#define MODE_2PL    (1)
#define MODE_OCC    (2)
#define MODE_RCC    (3)
#define MODE_RPC_NULL   (4)

    // deprecated.
#define MODE_DEPTRAN (3)

#define OP_IR   (0x1)
#define OP_DR   (0x2)
#define OP_R    (0x3)
#define OP_W    (0x4)

#define EDGE_D  (0x1)
#define EDGE_I  (0x2)

#define RR  (0x0)
#define WW  (0x1)
#define RW  (0x2)
#define WR  (0x4)
#define IRW (0x2)
#define WIR (0x4)
#define DRW (0x1)
#define WDR (0x1)


/** TPCA */
#define TPCA (0)


/** TPCC */
#define TPCC (1)


/** RW Benchmark */
#define RW_BENCHMARK (2)


/** TPCC */
#define TPCC_DIST_PART (3)

/** TPCC */
#define TPCC_REAL_DIST_PART (4)

/** micro Benchmark */
#define MICRO_BENCH (5)

}

#endif // CONSTANTS_HPP_
