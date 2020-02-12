#pragma once

#include <ostream>
#include <string>

#include "blob.h"
#include "utils.h"

namespace mdb {

class Value {
    friend std::ostream& operator<< (std::ostream& o, const Value& v);
public:
    uint64_t ver_ = 0;

    typedef enum {
        UNKNOWN,
        I32,
        I64,
        DOUBLE,
        STR
    } kind;

    Value(): k_(UNKNOWN) {}
    explicit Value(i32 v): k_(I32), i32_(v) {}
    explicit Value(i64 v): k_(I64), i64_(v) {}
    explicit Value(double v): k_(DOUBLE), double_(v) {}
    explicit Value(const std::string& s): k_(STR), p_str_(new std::string(s)) {}
    explicit Value(const char* str): k_(STR), p_str_(new std::string(str)) {}

    Value(Value&& o) noexcept {
        k_ = o.k_;
        i32_ = o.i32_;
        i64_ = o.i64_;
        double_ = o.double_;
        if (k_ == STR) {
            p_str_ = new std::string(std::move(*o.p_str_));
            o.p_str_ = nullptr;
        }
    }

    Value(const Value& o) {
        k_ = o.k_;
        i32_ = o.i32_;
        i64_ = o.i64_;
        double_ = o.double_;
        if (k_ == STR) {
            p_str_ = new std::string(*o.p_str_);
        }
    }

    ~Value() {
        if (k_ == STR) {
            delete p_str_;
        }
    }

    // let's add a move assign operator for ACCESS
    Value& operator=(Value&& that) noexcept {
        verify(this != &that);
        if (k_ == STR) {
            delete p_str_;
        }
        k_ = that.k_;
        i32_ = that.i32_;
        i64_ = that.i64_;
        double_ = that.double_;
        if (k_ == STR) {
            p_str_ = new std::string(std::move(*that.p_str_));
            that.p_str_ = nullptr;
        }
        return *this;
    }

    const Value& operator= (const Value& o) {
        if (this != &o) {
            if (k_ == STR) {
                delete p_str_;
            }
            k_ = o.k_;
            i32_ = o.i32_;
            i64_ = o.i64_;
            double_ = o.double_;
            if (k_ == STR) {
                p_str_ = new std::string(*o.p_str_);
            }
        }
        return *this;
    }
    const Value& operator= (i32 v) {
        this->set_i32(v);
        return *this;
    }
    const Value& operator= (i64 v) {
        this->set_i64(v);
        return *this;
    }
    const Value& operator= (double v) {
        this->set_double(v);
        return *this;
    }
    const Value& operator= (const std::string& s) {
        this->set_str(s);
        return *this;
    }
    const Value& operator= (const char* str) {
        this->set_str(str);
        return *this;
    }

    // -1: this < o, 0: this == o, 1: this > o
    // UNKNOWN == UNKNOWN
    // both side should have same kind
    int compare(const Value& o) const;

    bool operator ==(const Value& o) const {
        return compare(o) == 0;
    }
    bool operator !=(const Value& o) const {
        return compare(o) != 0;
    }
    bool operator <(const Value& o) const {
        return compare(o) == -1;
    }
    bool operator >(const Value& o) const {
        return compare(o) == 1;
    }
    bool operator <=(const Value& o) const {
        return compare(o) != 1;
    }
    bool operator >=(const Value& o) const {
        return compare(o) != -1;
    }

    kind get_kind() const {
        return k_;
    }

    i32 get_i32() const {
        verify(k_ == I32);
        return i32_;
    }

    i64 get_i64() const {
        verify(k_ == I64 || k_ == I32);
        switch (k_) {
            case I64:
                return i64_;
            case I32:
                return static_cast<i64>(i32_);
            default:
                verify(0);
        }
        verify(0);
        return 0;
    }

    double get_double() const {
        verify(k_ == DOUBLE);
        return double_;
    }

    const std::string& get_str() const {
        verify(k_ == STR);
        return *p_str_;
    }

    void set_i32(i32 v) {
        if (k_ == UNKNOWN) {
            k_ = I32;
        }
        verify(k_ == I32);
        i32_ = v;
    }

    void set_i64(i64 v) {
        if (k_ == UNKNOWN) {
            k_ = I64;
        }
        verify(k_ == I64);
        i64_ = v;
    }

    void set_double(double v) {
        if (k_ == UNKNOWN) {
            k_ = DOUBLE;
        }
        verify(k_ == DOUBLE);
        double_ = v;
    }

    void set_str(const std::string& str) {
        if (k_ == UNKNOWN) {
            k_ = STR;
            p_str_ = new std::string(str);
        } else {
            verify(k_ == STR);
            *p_str_ = str;
        }
    }

    void write_binary(char* buf) const;

    blob get_blob() const;

private:
    kind k_;

    union {
        i32 i32_;
        i64 i64_;
        double double_;
        std::string* p_str_;
    };
};

std::ostream& operator<< (std::ostream& o, const Value& v);
std::string to_string(const Value& v);

} // namespace mdb
