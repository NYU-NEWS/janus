

#include "marshallable.h"
#include "rcc/dep_graph.h"

namespace janus {

int Marshallable::RegInitializer(int32_t cmd_type,
                                 function<Marshallable * ()> init) {
  auto pair = Initializers().insert(std::make_pair(cmd_type, init));
  verify(pair.second);
}

function<Marshallable * ()> &
Marshallable::GetInitializer(int32_t type) {
  auto it = Initializers().find(type);
  verify(it != Initializers().end());
  return it->second;
}

map <int32_t, function<Marshallable * ()>> &
Marshallable::Initializers() {
  static map <int32_t, function<Marshallable * ()>> m;
  return m;
};

Marshal &Marshallable::FromMarshal(Marshal &m) {
  verify(0);
}

Marshal &Marshallable::CreateActuallObjectFrom(Marshal &m) {
  verify(data_.get() == nullptr);
  switch (rtti_) {
    case Marshallable::RCC_GRAPH:
      data_.reset(new RccGraph());
      break;
    case Marshallable::EMPTY_GRAPH:
      data_.reset(new EmptyGraph);
      break;
    case Marshallable::UNKNOWN:
      verify(0);
      break;
    default:
      auto &func = GetInitializer(rtti_);
      data_.reset(func());
      verify(0);
      break;
  }
  data_->FromMarshal(m);
}

Marshal &Marshallable::ToMarshal(Marshal &m) const {
  verify(0);
  return m;
}

} // namespace janus