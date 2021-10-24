#ifndef __HASH_H
#define __HASH_H

#include <openssl/sha.h>

#define HASH_LENGTH	SHA512_DIGEST_LENGTH

static inline void hashfn(const unsigned char *d, size_t n, unsigned char *md)
{
	SHA512(d, n, md);
}


#endif
