

#include <mutex>
#include "marshallable.h"
#include "rococo/dep_graph.h"

namespace janus {

static std::mutex md_mutex_g;

int MarshallDeputy::RegInitializer(int32_t cmd_type,
                                   function<Marshallable * ()> init) {
  md_mutex_g.lock();
  auto pair = Initializers().insert(std::make_pair(cmd_type, init));
  verify(pair.second);
  md_mutex_g.unlock();
  return 0;
}

function<Marshallable * ()> &
MarshallDeputy::GetInitializer(int32_t type) {
  md_mutex_g.lock();
  auto it = Initializers().find(type);
  verify(it != Initializers().end());
  auto& f = it->second;
  md_mutex_g.unlock();
  return f;
}

map <int32_t, function<Marshallable * ()>> &
MarshallDeputy::Initializers() {
  static map <int32_t, function<Marshallable * ()>> m;
  return m;
};

Marshal &Marshallable::FromMarshal(Marshal &m) {
  verify(0);
}

Marshal& MarshallDeputy::CreateActualObjectFrom(Marshal& m) {
  verify(sp_data_ == nullptr);
  switch (kind_) {
    case RCC_GRAPH:
      sp_data_.reset(new RccGraph());
      break;
    case EMPTY_GRAPH:
      sp_data_.reset(new EmptyGraph());
      break;
    case UNKNOWN:
      verify(0);
      break;
    default:
      auto &func = GetInitializer(kind_);
      verify(func);
      sp_data_.reset(func());
      break;
  }
  verify(sp_data_);
  sp_data_->FromMarshal(m);
  verify(sp_data_->kind_);
  verify(kind_);
  verify(sp_data_->kind_ == kind_);
  return m;
}

Marshal &Marshallable::ToMarshal(Marshal &m) const {
  verify(0);
  return m;
}

} // namespace janus