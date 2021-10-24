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

static int srcfd;

static void split(FILE *fp, uint64_t from, uint64_t to)
{
	uint64_t length;
	uint8_t *buf;
	unsigned char sha512[SHA512_DIGEST_LENGTH];
	int i;

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

	if (xpread(srcfd, buf, length, from) != length) {
		fprintf(stderr, "read error\n");
		exit(EXIT_FAILURE);
	}

	SHA512(buf, length, sha512);

	free(buf);

	for (i = 0; i < sizeof(sha512); i++)
		fprintf(fp, "%.2x", sha512[i]);
	fprintf(fp, " %" PRId64 "\n", length);
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

static void split_cb(void *cookie, int num, uint64_t *split_offsets)
{
	static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
	char *ptr;
	size_t size;
	FILE *fp;
	int i;

	fp = open_memstream(&ptr, &size);

	for (i = 0; i < num; i++)
		split(fp, split_offsets[i], split_offsets[i + 1]);

	fclose(fp);

	pthread_mutex_lock(&lock);
	if (xwrite(1, ptr, size) != size)
		exit(EXIT_FAILURE);
	pthread_mutex_unlock(&lock);
}

int main(int argc, char *argv[])
{
	int i;

	if (argc < 2) {
		fprintf(stderr, "syntax: %s <file>+\n", argv[0]);
		return 1;
	}

	for (i = 1; i < argc; i++) {
		struct split_job sj;

		srcfd = open(argv[i], O_RDONLY);
		if (srcfd < 0) {
			perror("open");
			continue;
		}

		sj.fd = srcfd;
		sj.crc_block_size = 64;
		sj.crc_thresh = 0x00001000;
		sj.cookie = NULL;
		sj.handler_split = split_cb;
		do_split(&sj);

		close(srcfd);
	}

	return 0;
}
