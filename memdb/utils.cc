#include <unistd.h>

#include "MurmurHash3.h"
#include "xxhash.h"

#include "utils.h"

namespace mdb {

uint32_t stringhash32(const void* data, int len) {
    uint32_t hash_value;
    static int seed = getpid();
    // MurmurHash3_x86_32(data, len, seed, &hash_value);
    hash_value = XXH32(data, len, seed);
    return hash_value;
}

uint64_t stringhash64(const void* data, int len) {
    uint64_t hash_value[2];
    static int seed = getpid();
    MurmurHash3_x64_128(data, len, seed, hash_value);
    return hash_value[0] ^ hash_value[1];
}

// from MurmurHash3
static inline uint32_t fmix32(uint32_t h) {
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h;
}

// from MurmurHash3
static inline uint64_t fmix64(uint64_t k) {
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccd;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53;
    k ^= k >> 33;
    return k;
}

uint32_t inthash32(uint32_t* key, int key_length) {
    static int seed = getpid();
    uint32_t h = seed;
    // http://cstheory.stackexchange.com/questions/9639/how-did-knuth-derive-a
    const uint32_t A = 0x9e3779b9;
    for (int i = 0; i < key_length; i++) {
        h = (h * A) ^ fmix32(key[i]);
    }
    return h;
}

uint64_t inthash64(uint64_t* key, int key_length) {
    static int seed = getpid();
    uint64_t h = seed;
    const uint64_t A = 0x9e3779b97f4a7c15;  // obtained from wolframalpha
    for (int i = 0; i < key_length; i++) {
        h = (h * A) ^ fmix64(key[i]);
    }
    return h;
}

} // namespace mdb
