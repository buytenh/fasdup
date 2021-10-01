all:		show split

clean:
		rm -f show
		rm -f split

show:		show.c common.c common.h crc32c.c crc32c.h splitpoints.c splitpoints.h
		gcc -D_FILE_OFFSET_BITS=64 -Wall -o show -pthread show.c common.c crc32c.c splitpoints.c

split:		split.c common.c common.h crc32c.c crc32c.h splitpoints.c splitpoints.h
		gcc -D_FILE_OFFSET_BITS=64 -Wall -o split -pthread split.c common.c crc32c.c splitpoints.c
