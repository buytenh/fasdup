#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <openssl/sha.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "common.h"
#include "splitpoints.h"

static char hexnibble(int n)
{
	if (n < 10)
		return '0' + n;
	else
		return 'a' + (n - 10);
}

static void split(FILE *fp, int fd, uint64_t from, uint64_t to)
{
	uint64_t length;
	uint8_t *buf;
	unsigned char sha512[SHA512_DIGEST_LENGTH];
	int len;
	int i;
	char pbuf[256];

	length = to - from;
	if (length > SSIZE_MAX) {
		fprintf(stderr, "fragment too big (%" PRId64 ")\n", length);
		exit(EXIT_FAILURE);
	}

	buf = malloc(length);
	if (buf == NULL) {
		fprintf(stderr, "out of memory\n");
		exit(EXIT_FAILURE);
	}

	if (xpread(fd, buf, length, from) != length) {
		fprintf(stderr, "read error\n");
		exit(EXIT_FAILURE);
	}

	SHA512(buf, length, sha512);

	free(buf);

	len = 0;
	for (i = 0; i < sizeof(sha512); i++) {
		pbuf[len++] = hexnibble(sha512[i] >> 4);
		pbuf[len++] = hexnibble(sha512[i] & 0xf);
	}
	len += sprintf(pbuf + len, " %" PRId64 "\n", length);

	fwrite(pbuf, len, 1, fp);
}

static ssize_t xwrite(int fd, const void *buf, size_t count)
{
	off_t processed;

	processed = 0;
	while (processed < count) {
		ssize_t ret;

		do {
			ret = write(fd, buf, count - processed);
		} while (ret < 0 && errno == EINTR);

		if (ret < 0) {
			perror("write");
			return processed ? processed : ret;
		}

		buf += ret;
		processed += ret;
	}

	return processed;
}

static void split_cb(void *cookie, int fd, int num, uint64_t *split_offsets)
{
	static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
	char *ptr;
	size_t size;
	FILE *fp;
	int i;

	fp = open_memstream(&ptr, &size);

	for (i = 0; i < num; i++)
		split(fp, fd, split_offsets[i], split_offsets[i + 1]);

	fclose(fp);

	pthread_mutex_lock(&lock);
	if (xwrite(1, ptr, size) != size)
		exit(EXIT_FAILURE);
	pthread_mutex_unlock(&lock);

	free(ptr);
}

int main(int argc, char *argv[])
{
	int i;

	if (argc < 2) {
		fprintf(stderr, "syntax: %s <file>+\n", argv[0]);
		return 1;
	}

	for (i = 1; i < argc; i++) {
		int srcfd;
		struct split_job sj;

		srcfd = open(argv[i], O_RDONLY);
		if (srcfd < 0) {
			perror("open");
			continue;
		}

		sj.fd = srcfd;
		sj.file = argv[i];
		sj.crc_block_size = 64;
		sj.crc_thresh = 0x00001000;
		sj.cookie = NULL;
		sj.handler_split = split_cb;
		do_split(&sj);

		close(srcfd);
	}

	return 0;
}
