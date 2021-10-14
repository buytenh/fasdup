all:		countfrags hashfrags show split splitfs stripnewlines

clean:
		rm -f countfrags
		rm -f hashfrags
		rm -f show
		rm -f split
		rm -f splitfs
		rm -f stripnewlines

countfrags:	countfrags.c
		gcc -Wall -livykis -o countfrags countfrags.c

hashfrags:	hashfrags.c common.c common.h crc32c.c crc32c.h splitpoints.c splitpoints.h
		gcc -D_FILE_OFFSET_BITS=64 -Wall -lcrypto -o hashfrags -pthread hashfrags.c common.c crc32c.c splitpoints.c

show:		show.c common.c common.h crc32c.c crc32c.h splitpoints.c splitpoints.h
		gcc -D_FILE_OFFSET_BITS=64 -Wall -o show -pthread show.c common.c crc32c.c splitpoints.c

split:		split.c common.c common.h crc32c.c crc32c.h splitpoints.c splitpoints.h
		gcc -D_FILE_OFFSET_BITS=64 -Wall -o split -pthread split.c common.c crc32c.c splitpoints.c

splitfs:	splitfs.c
		gcc -O6 -Wall -g -o splitfs splitfs.c `pkg-config fuse3 --cflags --libs` `pkg-config ivykis --cflags --libs`

stripnewlines:	stripnewlines.c
		gcc -Wall -o stripnewlines stripnewlines.c
