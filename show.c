#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "splitpoints.h"

static void split(void *cookie, off_t split_offset)
{
	printf("%jd\n", (intmax_t)split_offset);
	fflush(stdout);
}

int main(int argc, char *argv[])
{
	int srcfd;
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

	sj.fd = srcfd;
	sj.crc_block_size = 64;
	sj.crc_thresh = 0x00001000;
	sj.cookie = NULL;
	sj.handler_split = split;
	do_split(&sj);

	close(srcfd);

	return 0;
}
