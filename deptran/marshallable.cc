

#include "marshallable.h"
#include "rococo/dep_graph.h"

namespace janus {

int MarshallDeputy::RegInitializer(int32_t cmd_type,
                                 function<Marshallable * ()> init) {
  auto pair = Initializers().insert(std::make_pair(cmd_type, init));
  verify(pair.second);
}

function<Marshallable * ()> &
MarshallDeputy::GetInitializer(int32_t type) {
  auto it = Initializers().find(type);
  verify(it != Initializers().end());
  return it->second;
}

map <int32_t, function<Marshallable * ()>> &
MarshallDeputy::Initializers() {
  static map <int32_t, function<Marshallable * ()>> m;
  return m;
};

Marshal &Marshallable::FromMarshal(Marshal &m) {
  verify(0);
}

Marshal &MarshallDeputy::CreateActuallObjectFrom(Marshal &m) {
  verify(data_ == nullptr);
  manage_memory_ = true;
  switch (kind_) {
    case RCC_GRAPH:
      data_ = new RccGraph();
      break;
    case EMPTY_GRAPH:
      data_ = new EmptyGraph();
      break;
    case UNKNOWN:
      verify(0);
      break;
    default:
      auto &func = GetInitializer(kind_);
      data_ = func();
      verify(0);
      break;
  }
  data_->FromMarshal(m);
  data_->kind_ = kind_;
}

Marshal &Marshallable::ToMarshal(Marshal &m) const {
  verify(0);
  return m;
}

} // namespace janus