#pragma once
#include <tuple>
#include "constants.h"

namespace mdcc {
  enum BallotType { FAST, CLASSIC };
  struct Ballot {
    ballot_t number;
    BallotType type;
    Ballot(ballot_t number, BallotType type) : number(number), type(type) {}
    Ballot() : Ballot(0, FAST) {}
  protected:
    std::tuple<const ballot_t&, const BallotType&> ToTuple() const {
      return std::tie(this->number, this->type);
    };
  public:
    bool operator==(const Ballot& other) const { return ToTuple() == other.ToTuple(); }
    bool operator!=(const Ballot& other) const { return !(*this == other); }
    bool operator< (const Ballot& other) const { return ToTuple() < other.ToTuple(); }
    bool operator> (const Ballot& other) const { return other.ToTuple() < ToTuple(); }
    bool operator<=(const Ballot& other) const { return !(other > *this); }
    bool operator>=(const Ballot& other) const { return !(*this < other); }
    std::string string() const {
      std::ostringstream ss;
      ss << "[ballot " << number << "; type " << type << "]";
      return ss.str();
    }
  };

  inline rrr::Marshal& operator <<(rrr::Marshal& m, const Ballot& b) {
    m << b.number;
    m << static_cast<int>(b.type);
    return m;
  }

  inline rrr::Marshal& operator >>(rrr::Marshal& m, Ballot& b) {
    int tval;
    m >> b.number;
    m >> tval;
    b.type = static_cast<BallotType>(tval);
    return m;
  }
}
