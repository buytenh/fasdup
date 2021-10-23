#include <stdio.h>
#include <stdlib.h>
#include <iv_avl.h>
#include <iv_list.h>
#include <openssl/sha.h>
#include <string.h>

#define ROUND_UP(x, y)	((((x) + (y) - 1) / (y)) * (y))

struct frag {
	struct iv_avl_node	an;
	uint8_t			sha512[SHA512_DIGEST_LENGTH];
	uint64_t		length;
	int			count;
};

static struct iv_avl_tree frags;

static int
compare_frags(const struct iv_avl_node *_a, const struct iv_avl_node *_b)
{
	const struct frag *a;
	const struct frag *b;

	a = iv_container_of(_a, struct frag, an);
	b = iv_container_of(_b, struct frag, an);

	return memcmp(a->sha512, b->sha512, sizeof(a->sha512));
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

	for (i = 0; i < SHA512_DIGEST_LENGTH; i++) {
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

static struct frag *find_frag(const uint8_t *sha512)
{
	struct iv_avl_node *an;

	an = frags.root;
	while (an != NULL) {
		struct frag *f;
		int ret;

		f = iv_container_of(an, struct frag, an);

		ret = memcmp(sha512, f->sha512, sizeof(f->sha512));
		if (ret == 0)
			return f;

		if (ret < 0)
			an = an->left;
		else
			an = an->right;
	}

	return NULL;
}

static void count_frag(const uint8_t *sha512, uint64_t length)
{
	struct frag *f;

	f = find_frag(sha512);
	if (f != NULL) {
		if (length != f->length) {
			fprintf(stderr, "fragment length mismatch!\n");
			exit(EXIT_FAILURE);
		}
		f->count++;
		return;
	}

	f = malloc(sizeof(*f));
	if (f == NULL) {
		fprintf(stderr, "out of memory!\n");
		exit(EXIT_FAILURE);
	}

	memcpy(f->sha512, sha512, sizeof(f->sha512));
	f->length = length;
	f->count = 1;
	iv_avl_tree_insert(&frags, &f->an);
}

static void read_frags(FILE *fp)
{
	while (1) {
		char line[256];
		char hash[256];
		uint64_t length;
		uint8_t sha512[SHA512_DIGEST_LENGTH];

		if (fgets(line, sizeof(line), fp) == NULL)
			break;

		if (sscanf(line, "%255s %" PRId64, hash, &length) != 2) {
			fprintf(stderr, "can't parse line: %s", line);
			exit(EXIT_FAILURE);
		}

		if (strlen(hash) != 2 * SHA512_DIGEST_LENGTH) {
			fprintf(stderr, "can't parse hash [%s]\n", hash);
			exit(EXIT_FAILURE);
		}

		if (parse_hash(sha512, hash)) {
			fprintf(stderr, "can't parse hash [%s]\n", hash);
			exit(EXIT_FAILURE);
		}

		count_frag(sha512, length);
	}
}

static void print_summary(void)
{
	uint64_t frag_count;
	uint64_t unique_frag_count;
	uint64_t bytes;
	uint64_t unique_bytes;
	uint64_t pagebytes;
	uint64_t unique_pagebytes;
	struct iv_avl_node *an;

	frag_count = 0;
	unique_frag_count = 0;

	bytes = 0;
	unique_bytes = 0;

	pagebytes = 0;
	unique_pagebytes = 0;

	iv_avl_tree_for_each (an, &frags) {
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

	printf("fragments (total)\t%15" PRId64 "\n", frag_count);
	printf("fragments (unique)\t%15" PRId64 "\n", unique_frag_count);
	printf("bytes (total)\t\t%15" PRId64 "\n", bytes);
	printf("bytes (unique)\t\t%15" PRId64 "\n", unique_bytes);
	printf("bytes in pages (total)\t%15" PRId64 "\n", pagebytes);
	printf("bytes in pages (unique)\t%15" PRId64 "\n", unique_pagebytes);
}

int main(int argc, char *argv[])
{
	INIT_IV_AVL_TREE(&frags, compare_frags);

	if (argc > 1) {
		int i;

		for (i = 1; i < argc; i++) {
			FILE *fp;

			fp = fopen(argv[i], "r");
			if (fp == NULL) {
				perror("fopen");
				return 1;
			}

			read_frags(fp);

			fclose(fp);
		}
	} else {
		read_frags(stdin);
	}

	print_summary();

	return 0;
}
