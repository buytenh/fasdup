#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <iv_avl.h>
#include <iv_list.h>
#include <pthread.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "common.h"
#include "hash.h"

#define ROUND_UP(x, y)	((((x) + (y) - 1) / (y)) * (y))

#define TREES		16777216

static struct {
	struct iv_avl_tree	frags;
	pthread_mutex_t		lock;
} frags[TREES];

struct frag {
	struct iv_avl_node	an;
	uint8_t			hash[HASH_LENGTH];
	uint64_t		length;
	int			count;
};

static int
compare_frags(const struct iv_avl_node *_a, const struct iv_avl_node *_b)
{
	const struct frag *a;
	const struct frag *b;

	a = iv_container_of(_a, struct frag, an);
	b = iv_container_of(_b, struct frag, an);

	return memcmp(a->hash, b->hash, sizeof(a->hash));
}

static int hextoval(char c)
{
	if (c >= '0' && c <= '9')
		return c - '0';

	if (c >= 'A' && c <= 'F')
		return 10 + (c - 'A');

	if (c >= 'a' && c <= 'f')
		return 10 + (c - 'a');

	return -1;
}

static int parse_hash(uint8_t *hash, char *text)
{
	int i;

	for (i = 0; i < HASH_LENGTH; i++) {
		int val;
		int val2;

		val = hextoval(text[2 * i]);
		if (val < 0)
			return 1;

		val2 = hextoval(text[2 * i + 1]);
		if (val2 < 0)
			return 1;

		hash[i] = (val << 4) | val2;
	}

	return 0;
}

static struct frag *find_frag(struct iv_avl_tree *frags, const uint8_t *hash)
{
	struct iv_avl_node *an;

	an = frags->root;
	while (an != NULL) {
		struct frag *f;
		int ret;

		f = iv_container_of(an, struct frag, an);

		ret = memcmp(hash, f->hash, sizeof(f->hash));
		if (ret == 0)
			return f;

		if (ret < 0)
			an = an->left;
		else
			an = an->right;
	}

	return NULL;
}

static int hash_to_tree(const uint8_t *hash)
{
	return (hash[0] << 16) | (hash[1] << 8) | hash[2];
}

static void count_frag(const uint8_t *hash, uint64_t length)
{
	int tree;
	struct frag *f;

	tree = hash_to_tree(hash);
	pthread_mutex_lock(&frags[tree].lock);

	f = find_frag(&frags[tree].frags, hash);
	if (f != NULL) {
		if (length != f->length) {
			fprintf(stderr, "fragment length mismatch!\n");
			exit(EXIT_FAILURE);
		}
		f->count++;
		pthread_mutex_unlock(&frags[tree].lock);
		return;
	}

	f = malloc(sizeof(*f));
	if (f == NULL) {
		fprintf(stderr, "out of memory!\n");
		exit(EXIT_FAILURE);
	}

	memcpy(f->hash, hash, sizeof(f->hash));
	f->length = length;
	f->count = 1;
	iv_avl_tree_insert(&frags[tree].frags, &f->an);

	pthread_mutex_unlock(&frags[tree].lock);
}

static void count_frags(char *buf, size_t len)
{
	char *end;

	end = buf + len;
	while (buf < end) {
		char *n;
		char hashstr[256];
		uint64_t frag_length;
		uint8_t hash[HASH_LENGTH];

		n = memchr(buf, '\n', end - buf);
		if (n == NULL) {
			fprintf(stderr, "no newline found\n");
			exit(EXIT_FAILURE);
		}

		*n = 0;

		if (sscanf(buf, "%255s %" PRId64, hashstr, &frag_length) != 2) {
			fprintf(stderr, "can't parse line: %s", buf);
			exit(EXIT_FAILURE);
		}

		if (strlen(hashstr) != 2 * HASH_LENGTH) {
			fprintf(stderr, "can't parse hash [%s]\n", buf);
			exit(EXIT_FAILURE);
		}

		if (parse_hash(hash, hashstr)) {
			fprintf(stderr, "can't parse hash [%s]\n", hashstr);
			exit(EXIT_FAILURE);
		}

		count_frag(hash, frag_length);

		buf = (char *)n + 1;
	}
}

struct read_job {
	int		fd;

	pthread_mutex_t	lock;
	const uint8_t	*prev;
	int		prev_length;
};

static ssize_t xread(int fd, void *buf, size_t count)
{
	off_t processed;

	processed = 0;
	while (processed < count) {
		ssize_t ret;

		do {
			ret = read(fd, buf, count - processed);
		} while (ret < 0 && errno == EINTR);

		if (ret <= 0) {
			if (ret < 0)
				perror("read");
			return processed ? processed : ret;
		}

		buf += ret;
		processed += ret;
	}

	return processed;
}

static void *read_thread(void *_me)
{
	struct worker_thread *me = _me;
	struct read_job *rj = me->cookie;
	uint8_t buf[1048576];

	while (1) {
		size_t len;
		ssize_t ret;
		uint8_t *n;
		size_t nextline;

		pthread_mutex_lock(&rj->lock);

		len = 0;
		if (rj->prev != NULL) {
			memmove(buf, rj->prev, rj->prev_length);
			len = rj->prev_length;
		}

		ret = xread(rj->fd, buf + len, sizeof(buf) - len);
		if (ret <= 0) {
			pthread_mutex_unlock(&rj->lock);
			break;
		}

		len += ret;

		n = memrchr(buf, '\n', len);
		if (n == NULL) {
			fprintf(stderr, "no newline found\n");
			exit(EXIT_FAILURE);
		}

		rj->prev = NULL;

		nextline = n - buf + 1;
		if (nextline < len) {
			rj->prev = buf + nextline;
			rj->prev_length = len - nextline;
			len = nextline;
		}

		pthread_mutex_unlock(&rj->lock);

		count_frags((char *)buf, len);
	}

	return NULL;
}

static void read_frags(int fd)
{
	struct read_job rj;

	rj.fd = fd;
	pthread_mutex_init(&rj.lock, NULL);
	rj.prev = NULL;
	rj.prev_length = 0;
	run_threads(read_thread, &rj);

	pthread_mutex_destroy(&rj.lock);
}

struct summarize_job {
	int		tree;

	pthread_mutex_t	lock;
	uint64_t	frag_count;
	uint64_t	unique_frag_count;
	uint64_t	bytes;
	uint64_t	unique_bytes;
	uint64_t	pagebytes;
	uint64_t	unique_pagebytes;
};

static void *summarize_thread(void *_me)
{
	struct worker_thread *me = _me;
	struct summarize_job *sj = me->cookie;

	pthread_mutex_lock(&sj->lock);

	while (1) {
		int i;
		uint64_t frag_count;
		uint64_t unique_frag_count;
		uint64_t bytes;
		uint64_t unique_bytes;
		uint64_t pagebytes;
		uint64_t unique_pagebytes;
		struct iv_avl_node *an;

		i = sj->tree;
		if (i == TREES)
			break;

		sj->tree++;

		pthread_mutex_unlock(&sj->lock);

		frag_count = 0;
		unique_frag_count = 0;
		bytes = 0;
		unique_bytes = 0;
		pagebytes = 0;
		unique_pagebytes = 0;

		iv_avl_tree_for_each (an, &frags[i].frags) {
			struct frag *f;
			uint64_t pb;

			f = iv_container_of(an, struct frag, an);

			pb = ROUND_UP(f->length, 4096);

			frag_count += f->count;
			unique_frag_count++;

			bytes += f->count * f->length;
			unique_bytes += f->length;

			pagebytes += f->count * pb;
			unique_pagebytes += pb;
		}

		pthread_mutex_lock(&sj->lock);

		sj->frag_count += frag_count;
		sj->unique_frag_count += unique_frag_count;
		sj->bytes += bytes;
		sj->unique_bytes += unique_bytes;
		sj->pagebytes += pagebytes;
		sj->unique_pagebytes += unique_pagebytes;
	}

	pthread_mutex_unlock(&sj->lock);

	return NULL;
}

static void print_summary(void)
{
	struct summarize_job sj;

	memset(&sj, 0, sizeof(sj));
	pthread_mutex_init(&sj.lock, NULL);
	run_threads(summarize_thread, &sj);

	pthread_mutex_destroy(&sj.lock);

	printf("fragments (total)\t%15" PRId64 "\n", sj.frag_count);
	printf("fragments (unique)\t%15" PRId64 "\n", sj.unique_frag_count);
	printf("bytes (total)\t\t%15" PRId64 "\n", sj.bytes);
	printf("bytes (unique)\t\t%15" PRId64 "\n", sj.unique_bytes);
	printf("bytes in pages (total)\t%15" PRId64 "\n", sj.pagebytes);
	printf("bytes in pages (unique)\t%15" PRId64 "\n", sj.unique_pagebytes);
}

int main(int argc, char *argv[])
{
	int i;

	for (i = 0; i < TREES; i++) {
		INIT_IV_AVL_TREE(&frags[i].frags, compare_frags);
		pthread_mutex_init(&frags[i].lock, NULL);
	}

	if (argc > 1) {
		int i;

		for (i = 1; i < argc; i++) {
			int fd;

			fd = open(argv[i], O_RDONLY);
			if (fd < 0) {
				perror("open");
				return 1;
			}

			read_frags(fd);

			close(fd);
		}
	} else {
		read_frags(0);
	}

	print_summary();

	return 0;
}
