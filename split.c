#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "splitpoints.h"

static int dirfd;

static void split(int srcfd, uint64_t from, uint64_t to)
{
	char file[64];
	int fd;
	off_t off;

	printf("%" PRId64 "\r", from);
	fflush(stdout);

	snprintf(file, sizeof(file), "%.16" PRIx64, from);

	fd = openat(dirfd, file, O_CREAT | O_TRUNC | O_WRONLY, 0666);
	if (fd < 0) {
		perror("openat");
		exit(EXIT_FAILURE);
	}

	off = from;
	while (off < to) {
		ssize_t ret;

		do {
			ret = copy_file_range(srcfd, &off, fd,
					      NULL, to - off, 0);
		} while (ret < 0 && errno == EINTR);

		if (ret < 0) {
			perror("copy_file_range");
			exit(EXIT_FAILURE);
		}

		if (ret == 0) {
			fprintf(stderr, "copy_file_range EOF\n");
			exit(EXIT_FAILURE);
		}
	}

	close(fd);
}

static void split_cb(void *cookie, int fd, int num, uint64_t *split_offsets)
{
	int i;

	for (i = 0; i < num; i++)
		split(fd, split_offsets[i], split_offsets[i + 1]);
}

int main(int argc, char *argv[])
{
	int srcfd;
	struct split_job sj;

	if (argc != 3) {
		fprintf(stderr, "syntax: %s <dstdir> <file>\n", argv[0]);
		return 1;
	}

	dirfd = open(argv[1], O_DIRECTORY | O_PATH);
	if (dirfd < 0) {
		perror("opendir");
		return 1;
	}

	srcfd = open(argv[2], O_RDONLY);
	if (srcfd < 0) {
		perror("open");
		return 1;
	}

	sj.fd = srcfd;
	sj.file = argv[2];
	sj.crc_block_size = 64;
	sj.crc_thresh = 0x00001000;
	sj.cookie = NULL;
	sj.handler_split = split_cb;
	do_split(&sj);

	printf("\n");

	close(srcfd);

	return 0;
}
