#include "sai_redis.h"

REDIS_GENERIC_QUAD(HASH,hash);
REDIS_GENERIC_QUAD(FINE_GRAINED_HASH,fine_grained_hash);

const sai_hash_api_t redis_hash_api = {
    REDIS_GENERIC_QUAD_API(hash)
    REDIS_GENERIC_QUAD_API(fine_grained_hash)
};
      
