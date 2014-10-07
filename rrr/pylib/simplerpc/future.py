from simplerpc import _pyrpc
from simplerpc.marshal import Marshal

class Future(object):
    def __init__(self, id, rep_types):
        self.id = id
        self.rep_types = rep_types
        self.wait_ok = False

    @property
    def result(self):
        self.wait()
        return self.rep

    @property
    def error_code(self):
        self.wait()
        return self.err_code

    def wait(self, timeout_sec=None):
        if self.wait_ok:
            return
        if timeout_sec is None:
            self.err_code, rep_marshal_id = _pyrpc.future_wait(self.id)
        else:
            timeout_msec = int(timeout_sec * 1000)
            self.err_code, rep_marshal_id = _pyrpc.future_timedwait(self.id, timeout_msec)
        results = []
        if rep_marshal_id != 0 and self.err_code == 0:
            rep_m = Marshal(id=rep_marshal_id)
            for ty in self.rep_types:
                results += rep_m.read_obj(ty),
        self.wait_ok = True
        if len(results) == 0:
            self.rep = None
        elif len(results) == 1:
            self.rep = results[0]
        else:
            self.rep = results
        return self.err_code
