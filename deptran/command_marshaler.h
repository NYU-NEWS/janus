
#include "__dep__.h"

namespace rococo {

class SimpleCommand;
class TxData;
rrr::Marshal &operator<<(rrr::Marshal &m, const SimpleCommand &cmd);
rrr::Marshal &operator>>(rrr::Marshal &m, SimpleCommand &cmd);

} // namespace rococo