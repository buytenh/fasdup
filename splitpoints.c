#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <stdbool.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdint.h>
#include <unistd.h>
#include "common.h"
#include "crc32c.h"

#define BLOCK_SIZE		1048576
#define CRC_BLOCK_SIZE		64

static uint32_t crc_thresh = 0x00001000;

static int fd;
static off_t file_size;
static off_t file_offset;
static void (*split_handler)(off_t split_offset);

static bool split_at(uint8_t *buf)
{
	uint32_t crc;

	crc = crc32c(0, buf, CRC_BLOCK_SIZE);
	if (crc <= crc_thresh)
		return true;

	return false;
}

static void *split_thread(void *_me)
{
	struct worker_thread *me = _me;
	int split_offsets_num;
	uint64_t *split_offsets;

	split_offsets_num = 16;

	split_offsets = malloc(split_offsets_num * sizeof(*split_offsets));
	if (split_offsets == NULL)
		exit(EXIT_FAILURE);

	while (1) {
		off_t off;
		size_t toread;
		uint8_t buf[BLOCK_SIZE + CRC_BLOCK_SIZE - 1];
		ssize_t ret;
		int num;
		int i;

		xsem_wait(&me->sem0);

		off = file_offset;
		if (off == file_size) {
			xsem_post(&me->next->sem0);
			break;
		}

		file_offset += BLOCK_SIZE;
		if (file_offset > file_size)
			file_offset = file_size;

		toread = file_size - off;
		if (toread > sizeof(buf))
			toread = sizeof(buf);

		ret = xpread(fd, buf, toread, off);

		xsem_post(&me->next->sem0);

		if (ret != toread)
			exit(EXIT_FAILURE);

		num = 0;
		for (i = 0; i <= toread - CRC_BLOCK_SIZE; i++) {
			if (!split_at(buf + i))
				continue;

			if (num == split_offsets_num) {
				split_offsets_num *= 2;

				split_offsets = realloc(split_offsets,
					split_offsets_num *
						sizeof(*split_offsets));
				if (split_offsets == NULL)
					exit(EXIT_FAILURE);
			}
			split_offsets[num++] = off + i;
		}

		xsem_wait(&me->sem1);

		for (i = 0; i < num; i++)
			split_handler(split_offsets[i]);

		xsem_post(&me->next->sem1);
	}

	return NULL;
}

static void do_split(int fd_, void (*handler)(off_t))
{
	struct stat statbuf;

	if (fstat(fd_, &statbuf) < 0)
		exit(EXIT_FAILURE);

	fd = fd_;
	file_size = statbuf.st_size;
	file_offset = 0;
	split_handler = handler;
	run_threads(split_thread, NULL);
}

static void handler(off_t offset)
{
	printf("%jd\n", (intmax_t)offset);
}

int main(int argc, char *argv[])
{
	int fd;

	if (argc != 2) {
		fprintf(stderr, "syntax: %s <file>\n", argv[0]);
		return 1;
	}

	fd = open(argv[1], O_RDONLY);
	if (fd < 0) {
		perror("open");
		return 1;
	}

	do_split(fd, handler);

	close(fd);

	return 0;
}
