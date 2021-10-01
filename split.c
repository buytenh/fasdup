#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "splitpoints.h"

static int dirfd;
static int srcfd;
static off_t last_offset;

static void split(void *cookie, off_t split_offset)
{
	char file[64];
	int fd;

	printf("%jd..%jd\n", (intmax_t)last_offset, (intmax_t)split_offset - 1);
	fflush(stdout);

	snprintf(file, sizeof(file), "%.16jx", (intmax_t)last_offset);

	fd = openat(dirfd, file, O_CREAT | O_TRUNC | O_WRONLY, 0666);
	if (fd < 0) {
		perror("openat");
		exit(EXIT_FAILURE);
	}

	while (last_offset < split_offset) {
		ssize_t ret;

		do {
			ret = copy_file_range(srcfd, &last_offset, fd, NULL,
					      split_offset - last_offset, 0);
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

	if (last_offset != split_offset) {
		fprintf(stderr, "mismatch %jd vs %jd\n", last_offset,
			split_offset);
		exit(EXIT_FAILURE);
	}
}

int main(int argc, char *argv[])
{
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
