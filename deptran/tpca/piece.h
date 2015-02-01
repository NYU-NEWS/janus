#ifndef TPCA_PIE_H_
#define TPCA_PIE_H_

#include "all.h"

namespace rococo {

extern char TPCA_BRANCH[];
extern char TPCA_TELLER[];
extern char TPCA_CUSTOMER[];

#define TPCA_PAYMENT (0)
#define TPCA_PAYMENT_NAME "PAYMENT"

#define TPCA_PAYMENT_1 (0)
#define TPCA_PAYMENT_2 (1)
#define TPCA_PAYMENT_3 (2)

class TpcaPiece : public Piece {

public:

    void reg_all();

    void reg_pieces();

    void reg_lock_oracles();
};

}

#endif // TPCA_PIE_H_
