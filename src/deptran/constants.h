#pragma once
#include <stdint.h>

namespace janus {

#define ballot_t int64_t
#define cooid_t uint32_t
#define rank_t int32_t
#define txid_t uint64_t
#define txnid_t uint64_t
#define cmdid_t uint64_t // txnid and cmdid are the same thing
#define innid_t uint32_t
#define parid_t uint32_t
#define shardid_t uint32_t
#define locid_t uint32_t
#define svrid_t uint16_t
#define cliid_t uint16_t
#define siteid_t uint16_t
#define slotid_t uint64_t
#define phase_t uint32_t
#define epoch_t uint32_t
#define status_t uint32_t
#define txntype_t uint32_t
#define cmdtype_t uint32_t
#define groupid_t uint32_t

/** ACCESS related macro **/
// data types
#define snapshotid_t uint64_t  // access: ssid
#define acc_status_t uint8_t   // status is either "unchecked," "validating", or "finalized"
// record status
#define UNCHECKED (0x0)        // a txn is not checked yet
#define VALIDATING (0x1)       // Being validated, e.g., have received validation message
#define FINALIZED (0x2)        // finalized
#define ABORTED (0x3)          // txn aborted
// failure handling status
#define txn_status_t uint8_t
#define UNCLEARED (0x0)
#define CLEARED (0x1)
// use FINALIZED and ABORTED in record status
#define FAILURE_TIMEOUT (3000000) // failure timeout = 3 seconds

// dispatch responses
#define OFFSET_INVALID (0x1)   // for offset-1 optimization
#define NOT_DECIDED (0x2)      // some write reads returning has not been decided
#define BOTH_NEGATIVE (0x3)    // both offset invalid and not decided
#define EARLY_ABORT (0x4)            // early abort

//~also returns SUCCESS by default
// validate responses
#define CONSISTENT (0x1)       // return vale of is_consistent of AccDispatch RPC -- local ssids same
#define INCONSISTENT (0x0)     // validation gets inconsistent
//~also returns NOT_DECIDED upon validation
// obsolete below
//#define VALIDATE_ABORT (0x2)   // should not do validate, abort early!
/* ACCESS ML engine related below */
#define optype_t uint8_t
#define READ_REQ (0x0)      // if a piece only contains reads
#define WRITE_REQ (0x1)     // if a piece contains any write
#define UNDEFINED (0x2)     // if not specified by the workload

// for LFC ROTXN
#define TS_INDEX (30000)

/** read and write type */
#define OP_WRITE   (0x01)
#define OP_READ    (0x02)
#define OP_REREAD  (0x04)

/** transaction type */
#define TXN_UNKNOWN (0x00)
//#define TXN_START   (0x01)
//#define TXN_FINISH  (0x02)
//#define TXN_COMMIT  (0x04)
//#define TXN_ABORT   (0x08)

// a transaction (command) status
#define TXN_UKN (0x00)  // unknown
#define TXN_STD (0x01)  // started/dispatched
#define TXN_PAC (0x02)  // pre-accepted
#define TXN_ACC (0x04)  // accepted
#define TXN_CMT (0x08)  // committing
#define TXN_ALL_PREDECESORS_COMMITING (0x10)
#define TXN_DCD (0x20)  // decided
#define TXN_FNS (0x20)  // finished
#define TXN_ABT (0x40)  // aborted

#define TXN_BYPASS     (0x01)
#define TXN_SAFE       (0x02)
#define TXN_INSTANT    (0x02)
#define TXN_IMMEDIATE  (0x02)
#define TXN_DEFERRED   (0x04)
#define TXOP_MULTIHOP        (0x08) // for multi-hop data/flow dependency

#define DF_REAL (0x04)
#define DF_NO   (0x02)
#define DF_FAKE (0x0f)

#define RANK_UNDEFINED (0x00)
#define RANK_I (0x02)
#define RANK_D (0x04)
#define RANK_MAX (0x08)

#define SUCCESS     (0)
#define FAILURE     (-1)
#define CONTENTION  (-1)
#define REJECT      (-10)
#define ABSTAIN     (-10)
#define RETRY       (-10)
#define DELAYED     (1)

#define MODE_NONE   (0x00)
#define MODE_2PL    (0x01)
#define MODE_OCC    (0x02)
#define MODE_RCC    (0x04)
#define MODE_RO6    (0x08)
#define MODE_BRQ    (0x10)
#define MODE_JANUS    (0x10)
#define MODE_FEBRUUS    (0x20)
#define MODE_MDCC   (0x12)
#define MODE_TROAD    (0x03)
#define MODE_EXTERNC   (0x14)
#define MODE_RPC_NULL   (64)

    // deprecated.
#define MODE_DEPTRAN (4)

#define MODE_MULTI_PAXOS   (0x40)
#define MODE_EPAXOS        (0x80)
#define MODE_TAPIR         (0x100)
#define MODE_MENCIUS       (0x200)
#define MODE_NOT_READY     (0x00)
#define MODE_ACC           (0x33)

#define OP_IR   (0x1)
#define OP_DR   (0x2)
#define OP_R    (0x3)
#define OP_W    (0x4)

#define EDGE_ALL (0x0)
#define EDGE_I  (0x2)
#define EDGE_D  (0x4)

#define RR  (0x0)
#define WW  (0x1)
#define RW  (0x2)
#define WR  (0x4)
#define IRW (0x2)
#define WIR (0x4)
#define DRW (0x1)
#define WDR (0x1)


#define TPCA (0)
#define TPCC (1)
#define RW_BENCHMARK (2)
#define TPCC_DIST_PART (3)
#define TPCC_REAL_DIST_PART (4)
#define MICRO_BENCH (5)
#define FACEBOOK (6)
#define SPANNER (7)
#define DYNAMIC (8)


} // namespace janus
