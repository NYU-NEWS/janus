#pragma once
#include "__dep__.h"
#include "constants.h"
#include "marshallable.h"


namespace janus {
class CmdData : public Marshallable {
 public:
  cmdid_t id_ = 0;
  cmdtype_t type_ = 0;
  innid_t inn_id_ = 0;
  cmdid_t root_id_ = 0;
  cmdtype_t root_type_ = 0;

  virtual innid_t inn_id() const {
    return inn_id_;
  }
  virtual cmdtype_t type() {
    return type_;
  }
  virtual void Merge(CmdData&) {
    verify(0);
  }
  virtual set<parid_t>& GetPartitionIds() {
    verify(0);
    static set<parid_t> l;
    return l;
  }
  virtual void Reset() {
    verify(0);
  }
  virtual CmdData* Clone() const  {
    // deprecated.
    verify(0);
  };

  CmdData() : Marshallable(MarshallDeputy::CONTAINER_CMD) {}
  virtual ~CmdData() {};
  virtual Marshal& ToMarshal(Marshal&) const override;
  virtual Marshal& FromMarshal(Marshal&) override;
};
} // namespace janus
