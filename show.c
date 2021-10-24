#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "splitpoints.h"

static void split(void *cookie, int fd, int num, uint64_t *split_offsets)
{
	static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
	int i;

	pthread_mutex_lock(&lock);

	for (i = 0; i < num; i++)
		printf("%" PRId64 " ", split_offsets[i]);
	printf("-- %" PRId64 "\n", split_offsets[num]);

	pthread_mutex_unlock(&lock);
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
	sj.file = argv[1];
	sj.crc_block_size = 64;
	sj.crc_thresh = 0x00001000;
	sj.cookie = NULL;
	sj.handler_split = split;
	do_split(&sj);

	close(srcfd);

	return 0;
}
