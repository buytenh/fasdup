#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <limits.h>
#include <openssl/sha.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "common.h"
#include "splitpoints.h"

static int srcfd;
static off_t last_offset;

static void split(void *cookie, off_t split_offset)
{
	off_t length;
	uint8_t *buf;
	unsigned char sha512[SHA512_DIGEST_LENGTH];
	int i;

	length = split_offset - last_offset;
	if (length > SSIZE_MAX) {
		fprintf(stderr, "fragment too big (%jd)\n", length);
		exit(EXIT_FAILURE);
	}

	buf = malloc(length);
	if (buf == NULL) {
		fprintf(stderr, "out of memory\n");
		exit(EXIT_FAILURE);
	}

	if (xpread(srcfd, buf, length, last_offset) != length) {
		fprintf(stderr, "read error\n");
		exit(EXIT_FAILURE);
	}

	SHA512(buf, length, sha512);

	free(buf);

	for (i = 0; i < sizeof(sha512); i++)
		printf("%.2x", sha512[i]);
	printf(" %jd\n", length);
	fflush(stdout);

	last_offset = split_offset;
}

int main(int argc, char *argv[])
{
	struct split_job sj;

	if (argc != 2) {
		fprintf(stderr, "syntax: %s <file>\n", argv[0]);
		return 1;
	}

	srcfd = open(argv[1], O_RDONLY);
	if (srcfd < 0) {
		perror("open");
		return 1;
	}

	last_offset = 0;

	sj.fd = srcfd;
	sj.crc_block_size = 64;
	sj.crc_thresh = 0x00001000;
	sj.cookie = NULL;
	sj.handler_split = split;
	do_split(&sj);

	close(srcfd);

	return 0;
}
