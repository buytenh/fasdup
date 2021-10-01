all:		splitpoints

clean:
		rm -f splitpoints

splitpoints:	splitpoints.c common.c common.h crc32c.c crc32c.h
		gcc -D_FILE_OFFSET_BITS=64 -Wall -o splitpoints -pthread splitpoints.c common.c crc32c.c
