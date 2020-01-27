#pragma once

#include "rrr.hpp"

#include <errno.h>

// #include <math.h>

// optional %%: marks header section, code above will be copied into begin of generated C++ header
namespace benchmark {

struct point3 {
    double x;
    double y;
    double z;
};

inline rrr::Marshal& operator <<(rrr::Marshal& m, const point3& o) {
    m << o.x;
    m << o.y;
    m << o.z;
    return m;
}

inline rrr::Marshal& operator >>(rrr::Marshal& m, point3& o) {
    m >> o.x;
    m >> o.y;
    m >> o.z;
    return m;
}

class BenchmarkService: public rrr::Service {
public:
    enum {
        FAST_PRIME = 0x2b48b2bc,
        FAST_DOT_PROD = 0x4791b18a,
        FAST_ADD = 0x5fe459cf,
        FAST_NOP = 0x19244ffa,
        FAST_VEC = 0x1a49d53d,
        PRIME = 0x17096106,
        DOT_PROD = 0x37507c8e,
        ADD = 0x6276a651,
        NOP = 0x5c7079d6,
        SLEEP = 0x1f95d27f,
    };
    int __reg_to__(rrr::Server* svr) {
        int ret = 0;
        if ((ret = svr->reg(FAST_PRIME, this, &BenchmarkService::__fast_prime__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(FAST_DOT_PROD, this, &BenchmarkService::__fast_dot_prod__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(FAST_ADD, this, &BenchmarkService::__fast_add__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(FAST_NOP, this, &BenchmarkService::__fast_nop__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(FAST_VEC, this, &BenchmarkService::__fast_vec__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(PRIME, this, &BenchmarkService::__prime__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(DOT_PROD, this, &BenchmarkService::__dot_prod__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(ADD, this, &BenchmarkService::__add__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(NOP, this, &BenchmarkService::__nop__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(SLEEP, this, &BenchmarkService::__sleep__wrapper__)) != 0) {
            goto err;
        }
        return 0;
    err:
        svr->unreg(FAST_PRIME);
        svr->unreg(FAST_DOT_PROD);
        svr->unreg(FAST_ADD);
        svr->unreg(FAST_NOP);
        svr->unreg(FAST_VEC);
        svr->unreg(PRIME);
        svr->unreg(DOT_PROD);
        svr->unreg(ADD);
        svr->unreg(NOP);
        svr->unreg(SLEEP);
        return ret;
    }
    // these RPC handler functions need to be implemented by user
    // for 'raw' handlers, remember to reply req, delete req, and sconn->release(); use sconn->run_async for heavy job
    virtual void fast_prime(const rrr::i32& n, rrr::i8* flag);
    virtual void fast_dot_prod(const point3& p1, const point3& p2, double* v);
    virtual void fast_add(const rrr::v32& a, const rrr::v32& b, rrr::v32* a_add_b);
    virtual void fast_nop(const std::string&);
    virtual void fast_vec(const rrr::i32& n, std::vector<rrr::i64>* v);
    virtual void prime(const rrr::i32& n, rrr::i8* flag);
    virtual void dot_prod(const point3& p1, const point3& p2, double* v);
    virtual void add(const rrr::v32& a, const rrr::v32& b, rrr::v32* a_add_b);
    virtual void nop(const std::string&);
    virtual void sleep(const double& sec);
private:
    void __fast_prime__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        rrr::i32 in_0;
        req->m >> in_0;
        rrr::i8 out_0;
        this->fast_prime(in_0, &out_0);
        sconn->begin_reply(req);
        *sconn << out_0;
        sconn->end_reply();
        delete req;
        sconn->release();
    }
    void __fast_dot_prod__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        point3 in_0;
        req->m >> in_0;
        point3 in_1;
        req->m >> in_1;
        double out_0;
        this->fast_dot_prod(in_0, in_1, &out_0);
        sconn->begin_reply(req);
        *sconn << out_0;
        sconn->end_reply();
        delete req;
        sconn->release();
    }
    void __fast_add__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        rrr::v32 in_0;
        req->m >> in_0;
        rrr::v32 in_1;
        req->m >> in_1;
        rrr::v32 out_0;
        this->fast_add(in_0, in_1, &out_0);
        sconn->begin_reply(req);
        *sconn << out_0;
        sconn->end_reply();
        delete req;
        sconn->release();
    }
    void __fast_nop__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        std::string in_0;
        req->m >> in_0;
        this->fast_nop(in_0);
        sconn->begin_reply(req);
        sconn->end_reply();
        delete req;
        sconn->release();
    }
    void __fast_vec__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        rrr::i32 in_0;
        req->m >> in_0;
        std::vector<rrr::i64> out_0;
        this->fast_vec(in_0, &out_0);
        sconn->begin_reply(req);
        *sconn << out_0;
        sconn->end_reply();
        delete req;
        sconn->release();
    }
    void __prime__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        auto f = [=] {
            rrr::i32 in_0;
            req->m >> in_0;
            rrr::i8 out_0;
            this->prime(in_0, &out_0);
            sconn->begin_reply(req);
            *sconn << out_0;
            sconn->end_reply();
            delete req;
            sconn->release();
        };
        sconn->run_async(f);
    }
    void __dot_prod__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        auto f = [=] {
            point3 in_0;
            req->m >> in_0;
            point3 in_1;
            req->m >> in_1;
            double out_0;
            this->dot_prod(in_0, in_1, &out_0);
            sconn->begin_reply(req);
            *sconn << out_0;
            sconn->end_reply();
            delete req;
            sconn->release();
        };
        sconn->run_async(f);
    }
    void __add__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        auto f = [=] {
            rrr::v32 in_0;
            req->m >> in_0;
            rrr::v32 in_1;
            req->m >> in_1;
            rrr::v32 out_0;
            this->add(in_0, in_1, &out_0);
            sconn->begin_reply(req);
            *sconn << out_0;
            sconn->end_reply();
            delete req;
            sconn->release();
        };
        sconn->run_async(f);
    }
    void __nop__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        auto f = [=] {
            std::string in_0;
            req->m >> in_0;
            this->nop(in_0);
            sconn->begin_reply(req);
            sconn->end_reply();
            delete req;
            sconn->release();
        };
        sconn->run_async(f);
    }
    void __sleep__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        auto f = [=] {
            double in_0;
            req->m >> in_0;
            this->sleep(in_0);
            sconn->begin_reply(req);
            sconn->end_reply();
            delete req;
            sconn->release();
        };
        sconn->run_async(f);
    }
};

class BenchmarkProxy {
protected:
    rrr::Client* __cl__;
public:
    BenchmarkProxy(rrr::Client* cl): __cl__(cl) { }
    rrr::Future* async_fast_prime(const rrr::i32& n, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(BenchmarkService::FAST_PRIME, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << n;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 fast_prime(const rrr::i32& n, rrr::i8* flag) {
        rrr::Future* __fu__ = this->async_fast_prime(n);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *flag;
        }
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_fast_dot_prod(const point3& p1, const point3& p2, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(BenchmarkService::FAST_DOT_PROD, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << p1;
            *__cl__ << p2;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 fast_dot_prod(const point3& p1, const point3& p2, double* v) {
        rrr::Future* __fu__ = this->async_fast_dot_prod(p1, p2);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *v;
        }
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_fast_add(const rrr::v32& a, const rrr::v32& b, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(BenchmarkService::FAST_ADD, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << a;
            *__cl__ << b;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 fast_add(const rrr::v32& a, const rrr::v32& b, rrr::v32* a_add_b) {
        rrr::Future* __fu__ = this->async_fast_add(a, b);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *a_add_b;
        }
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_fast_nop(const std::string& in_0, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(BenchmarkService::FAST_NOP, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << in_0;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 fast_nop(const std::string& in_0) {
        rrr::Future* __fu__ = this->async_fast_nop(in_0);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_fast_vec(const rrr::i32& n, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(BenchmarkService::FAST_VEC, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << n;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 fast_vec(const rrr::i32& n, std::vector<rrr::i64>* v) {
        rrr::Future* __fu__ = this->async_fast_vec(n);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *v;
        }
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_prime(const rrr::i32& n, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(BenchmarkService::PRIME, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << n;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 prime(const rrr::i32& n, rrr::i8* flag) {
        rrr::Future* __fu__ = this->async_prime(n);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *flag;
        }
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_dot_prod(const point3& p1, const point3& p2, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(BenchmarkService::DOT_PROD, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << p1;
            *__cl__ << p2;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 dot_prod(const point3& p1, const point3& p2, double* v) {
        rrr::Future* __fu__ = this->async_dot_prod(p1, p2);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *v;
        }
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_add(const rrr::v32& a, const rrr::v32& b, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(BenchmarkService::ADD, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << a;
            *__cl__ << b;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 add(const rrr::v32& a, const rrr::v32& b, rrr::v32* a_add_b) {
        rrr::Future* __fu__ = this->async_add(a, b);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *a_add_b;
        }
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_nop(const std::string& in_0, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(BenchmarkService::NOP, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << in_0;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 nop(const std::string& in_0) {
        rrr::Future* __fu__ = this->async_nop(in_0);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_sleep(const double& sec, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(BenchmarkService::SLEEP, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << sec;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 sleep(const double& sec) {
        rrr::Future* __fu__ = this->async_sleep(sec);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
};

} // namespace benchmark


// optional %%: marks footer section, code below will be copied into end of generated C++ header

namespace benchmark {

inline void BenchmarkService::fast_dot_prod(const point3& p1, const point3& p2, double* v) {
    *v = p1.x * p2.x + p1.y * p2.y + p1.z * p2.z;
}

inline void BenchmarkService::fast_add(const rrr::v32& a, const rrr::v32& b, rrr::v32* a_add_b) {
    a_add_b->set(a.get() + b.get());
}

inline void BenchmarkService::prime(const rrr::i32& n, rrr::i8* flag) {
    return fast_prime(n, flag);
}

inline void BenchmarkService::dot_prod(const point3& p1, const point3& p2, double *v) {
    *v = p1.x * p2.x + p1.y * p2.y + p1.z * p2.z;
}

inline void BenchmarkService::add(const rrr::v32& a, const rrr::v32& b, rrr::v32* a_add_b) {
    a_add_b->set(a.get() + b.get());
}

inline void BenchmarkService::fast_nop(const std::string& str) {
    nop(str);
}

} // namespace benchmark

