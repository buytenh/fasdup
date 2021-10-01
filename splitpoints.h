#ifndef __SPLITPOINTS_H
#define __SPLITPOINTS_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

struct split_job {
	int		fd;
	size_t		crc_block_size;
	uint32_t	crc_thresh;
	void		*cookie;
	void		(*handler_split)(void *cookie, off_t split_offset);

	off_t		file_size;
	off_t		file_offset;
};

void do_split(struct split_job *sj);


#endif
