#ifndef __SPLITPOINTS_H
#define __SPLITPOINTS_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

struct split_job {
	int		fd;
	const char	*file;
	size_t		crc_block_size;
	uint32_t	crc_thresh;
	void		*cookie;
	void		(*handler_split)(void *cookie, int fd, int num,
					 uint64_t *split_offsets);

	uint64_t	file_size;
	uint64_t	file_offset;
	uint64_t	prev_splitpoint;
};

void do_split(struct split_job *sj);


#endif
