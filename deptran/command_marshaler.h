
#include "__dep__.h"

namespace rococo {

class SimpleCommand;
class ContainerCommand;
rrr::Marshal &operator<<(rrr::Marshal &m, const SimpleCommand &cmd);
rrr::Marshal &operator>>(rrr::Marshal &m, SimpleCommand &cmd);
rrr::Marshal &operator<<(rrr::Marshal &m, const ContainerCommand& cmd);
rrr::Marshal &operator>>(rrr::Marshal &m, ContainerCommand& cmd);

} // namespace rococo