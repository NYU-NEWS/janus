#pragma once
#include <tuple>
#include "constants.h"

namespace mdcc {
  enum BallotType { FAST, CLASSIC };
  struct Ballot {
    ballot_t number = 0;
    BallotType type = FAST;
    Ballot(ballot_t number, BallotType type) : number(number), type(type) {}
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
  };
}
