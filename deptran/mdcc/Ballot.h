#pragma once
#include <tuple>
#include "constants.h"

namespace mdcc {
  enum BallotType { FAST, CLASSIC };
  struct Ballot {
    siteid_t site_id;
    ballot_t number;
    BallotType type;
    Ballot(siteid_t site_id, ballot_t number, BallotType type) :
        number(number), type(type), site_id(site_id) {}
    Ballot() : Ballot(-1, 0, FAST) {}
  protected:
    std::tuple<const BallotType&, const ballot_t&, const siteid_t&> ToTuple() const {
      return std::tie(type, number, site_id);
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
      ss << "[ballot site " << site_id << "; number " << number << "; type " << type << "]";
      return ss.str();
    }
  };

  inline rrr::Marshal& operator <<(rrr::Marshal& m, const Ballot& b) {
    m << b.site_id;
    m << b.number;
    m << static_cast<int>(b.type);
    return m;
  }

  inline rrr::Marshal& operator >>(rrr::Marshal& m, Ballot& b) {
    int tval;
    m >> b.site_id;
    m >> b.number;
    m >> tval;
    b.type = static_cast<BallotType>(tval);
    return m;
  }
}
