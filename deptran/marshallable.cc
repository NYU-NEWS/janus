

#include "marshallable.h"
#include "rcc/dep_graph.h"

using namespace rococo;

Marshal& Marshallable::FromMarshal(Marshal& m) {
  verify(data_.get() == nullptr);
  switch (rtti_) {
    case Marshallable::RCC_GRAPH:
      data_.reset(new RccGraph());
      break;
    case Marshallable::EMPTY_GRAPH:
      data_.reset(new EmptyGraph);
      break;
    default:
      verify(0);
      break;
  }
  data_->FromMarshal(m);
}

Marshal& Marshallable::ToMarshal(Marshal& m) const {
  data_->ToMarshal(m);
  return m;
}