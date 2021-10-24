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
#include "splitpoints.h"

#define DIV_ROUND_UP(a, b)	(((a) + (b) - 1) / (b))

#define BLOCK_SIZE		16777216

static bool should_split_at(struct split_job *sj, uint8_t *buf)
{
#if 1
	uint32_t crc;

	crc = crc32c(0, buf, sj->crc_block_size);
	if (crc <= sj->crc_thresh)
		return true;

	return false;
#else
	if (buf[0] == '>')
		return true;

	return false;
#endif
}

static void *split_thread(void *_me)
{
	struct worker_thread *me = _me;
	struct split_job *sj = me->cookie;
	size_t buf_size;
	uint8_t *buf;
	size_t split_offsets_num;
	uint64_t *split_offsets;

	buf_size = BLOCK_SIZE + sj->crc_block_size - 1;

	buf = malloc(buf_size);
	if (buf == NULL)
		exit(EXIT_FAILURE);

	split_offsets_num = DIV_ROUND_UP(0x100000000LL, sj->crc_thresh);
	split_offsets_num = DIV_ROUND_UP(BLOCK_SIZE, split_offsets_num);
	split_offsets_num *= 4;

	split_offsets = malloc(split_offsets_num * sizeof(*split_offsets));
	if (split_offsets == NULL)
		exit(EXIT_FAILURE);

	while (1) {
		uint64_t off;
		size_t toread;
		ssize_t ret;
		size_t num;
		size_t i;

		xsem_wait(&me->sem0);

		off = sj->file_offset;
		if (off == sj->file_size) {
			xsem_post(&me->next->sem0);
			break;
		}

		sj->file_offset += BLOCK_SIZE;
		if (sj->file_offset > sj->file_size)
			sj->file_offset = sj->file_size;

		toread = sj->file_size - off;
		if (toread > buf_size)
			toread = buf_size;

		xsem_post(&me->next->sem0);

		ret = xpread(sj->fd, buf, toread, off);
		if (ret != toread)
			exit(EXIT_FAILURE);

		num = 1;
		for (i = off ? 0 : 1; i <= toread - sj->crc_block_size; i++) {
			if (!should_split_at(sj, buf + i))
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

		split_offsets[0] = sj->prev_splitpoint;
		sj->prev_splitpoint = split_offsets[num - 1];

		xsem_post(&me->next->sem1);

		if (num > 1)
			sj->handler_split(sj->cookie, num - 1, split_offsets);
	}

	free(split_offsets);
	free(buf);

	return NULL;
}

void do_split(struct split_job *sj)
{
	struct stat statbuf;
	uint64_t off[2];

	if (fstat(sj->fd, &statbuf) < 0)
		exit(EXIT_FAILURE);

	sj->file_size = statbuf.st_size;
	sj->file_offset = 0;
	sj->prev_splitpoint = 0;
	run_threads(split_thread, sj);

	off[0] = sj->prev_splitpoint;
	off[1] = sj->file_size;
	sj->handler_split(sj->cookie, 1, off);
}
