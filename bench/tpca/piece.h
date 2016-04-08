#pragma once

#include "deptran/piece.h"

namespace rococo {

extern char TPCA_BRANCH[];
extern char TPCA_TELLER[];
extern char TPCA_CUSTOMER[];

#define TPCA_PAYMENT (0)
#define TPCA_PAYMENT_NAME "PAYMENT"

#define TPCA_PAYMENT_1 (0)
#define TPCA_PAYMENT_2 (1)
#define TPCA_PAYMENT_3 (2)

#define TPCA_VAR_X   (100)
#define TPCA_VAR_Y   (200)
#define TPCA_VAR_Z   (300)

class TpcaPiece : public Piece {

public:

    void reg_all();

    void reg_pieces();
};

} // namespace rococo
