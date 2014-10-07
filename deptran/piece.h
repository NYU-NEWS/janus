#ifndef PIECE_H_
#define PIECE_H_

#include <string>

namespace rcc {

class Piece {
public:
    static Piece *get_piece(int benchmark);

    virtual void reg_all() = 0;

    virtual ~Piece() {}
};

} // namespace rcc

#endif
