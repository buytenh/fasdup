#define PACKAGE_VERSION		"0.1"

#define _FILE_OFFSET_BITS	64
#define _GNU_SOURCE

#define FUSE_USE_VERSION	32

#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>
#include <errno.h>
#include <fuse.h>
#include <iv_avl.h>
#include <iv_list.h>
#include <limits.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>

#define DIV_ROUND_UP(a, b)	(((a) + (b) - 1) / (b))

struct splitfs_file_info {
	int			fd;
	int			is_fragmented_file;
	struct iv_avl_tree	fragments;
	uint64_t		size;
};

struct splitfs_file_fragment {
	struct iv_avl_node	an;
	uint64_t		start;
	uint64_t		end;
};

static int backing_dir_fd;

static int is_fragmented_file_dir(int dirfd)
{
	struct stat buf;

	if (fstatat(dirfd, "0000000000000000", &buf, 0) < 0) {
		if (errno != ENOENT) {
			perror("fstatat");
			return -errno;
		}
		return 0;
	}

	if ((buf.st_mode & S_IFMT) != S_IFREG)
		return 0;

	return 1;
}

static int
iterate_fragmented_file_dir(int dirfd, void *cookie,
			    void (*handler)(void *cookie, uint64_t start,
					    uint64_t size))
{
	int fd;
	DIR *dirp;
	int ret;

	fd = openat(dirfd, ".", O_DIRECTORY | O_RDONLY);
	if (fd < 0) {
		perror("openat");
		ret = -errno;
		goto out;
	}

	dirp = fdopendir(fd);
	if (dirp == NULL) {
		perror("fdopendir");
		close(fd);
		ret = -errno;
		goto out;
	}

	ret = 0;
	while (1) {
		struct dirent *dent;
		uint64_t start;
		struct stat buf;
		int ret;

		errno = 0;

		dent = readdir(dirp);
		if (dent == NULL) {
			if (errno)
				perror("readdir");
			ret = -errno;
			break;
		}

		if (dent->d_type != DT_UNKNOWN && dent->d_type != DT_REG)
			continue;

		if (strlen(dent->d_name) != 16)
			continue;

		if (sscanf(dent->d_name, "%" PRIx64, &start) != 1)
			continue;

		ret = fstatat(dirfd, dent->d_name, &buf, 0);
		if (ret < 0) {
			perror("fstatat");
			continue;
		}

		if ((buf.st_mode & S_IFMT) != S_IFREG)
			continue;

		handler(cookie, start, buf.st_size);
	}

	closedir(dirp);

out:
	return ret;
}

static void file_size_handler(void *cookie, uint64_t start, uint64_t size)
{
	uint64_t *length = (uint64_t *)cookie;
	uint64_t end;

	end = start + size;
	if (end > *length)
		*length = end;
}

static int determine_fragmented_file_size(int dirfd, uint64_t *size)
{
	*size = 0;

	return iterate_fragmented_file_dir(dirfd, size, file_size_handler);
}

static const char *empty_path(const char *path)
{
	return path[1] ? (path + 1) : ".";
}

static int splitfs_getattr(const char *path, struct stat *buf,
			   struct fuse_file_info *fi)
{
	int fd;
	int ret;

	if (path[0] != '/') {
		fprintf(stderr, "getattr called with [%s]\n", path);
		return -ENOENT;
	}

	fd = openat(backing_dir_fd, empty_path(path), O_NOFOLLOW | O_RDONLY);
	if (fd < 0)
		return -errno;

	ret = fstat(fd, buf);
	if (ret < 0) {
		ret = -errno;
		close(fd);
		return ret;
	}

	if ((buf->st_mode & S_IFMT) == S_IFDIR) {
		ret = is_fragmented_file_dir(fd);
		if (ret < 0) {
			close(fd);
			return ret;
		}

		if (ret) {
			uint64_t size;

			ret = determine_fragmented_file_size(fd, &size);
			if (ret < 0) {
				close(fd);
				return ret;
			}

			buf->st_mode &= ~(S_IFMT | 0111);
			buf->st_mode |= S_IFREG;
			buf->st_size = size;
			buf->st_blocks = DIV_ROUND_UP(size, 512);
		}
	}

	close(fd);

	return 0;
}

static int splitfs_readlink(const char *path, char *buf, size_t bufsiz)
{
	int ret;

	if (path[0] != '/') {
		fprintf(stderr, "readlink called with [%s]\n", path);
		return -ENOENT;
	}

	ret = readlinkat(backing_dir_fd, path + 1, buf, bufsiz);
	if (ret < 0)
		return -errno;

	if (bufsiz && ret == bufsiz)
		ret--;
	buf[ret] = 0;

	return 0;
}

static int splitfs_truncate(const char *path, off_t length,
			    struct fuse_file_info *fi)
{
	return -EINVAL;
}

static int compare_file_fragments(const struct iv_avl_node *_a,
				  const struct iv_avl_node *_b)
{
	struct splitfs_file_fragment *a;
	struct splitfs_file_fragment *b;

	a = iv_container_of(_a, struct splitfs_file_fragment, an);
	b = iv_container_of(_b, struct splitfs_file_fragment, an);

	if (a->start < b->start)
		return -1;
	if (a->start > b->start)
		return 1;

	return 0;
}

static void file_frag_handler(void *cookie, uint64_t start, uint64_t size)
{
	struct splitfs_file_info *fh = cookie;
	struct splitfs_file_fragment *frag;

	frag = malloc(sizeof(*frag));
	if (frag == NULL) {
		fprintf(stderr, "out of memory\n");
		exit(EXIT_FAILURE);
	}

	frag->start = start;
	frag->end = start + size;
	iv_avl_tree_insert(&fh->fragments, &frag->an);

	if (frag->end > fh->size)
		fh->size = frag->end;
}

static void __free_file_fragment(struct iv_avl_node *an)
{
	struct splitfs_file_fragment *frag;

	frag = iv_container_of(an, struct splitfs_file_fragment, an);

	if (an->left != NULL)
		__free_file_fragment(an->left);
	if (an->right != NULL)
		__free_file_fragment(an->right);
	free(frag);
}

static void free_splitfs_file_info(struct splitfs_file_info *fh)
{
	close(fh->fd);
	if (fh->is_fragmented_file && fh->fragments.root != NULL)
		__free_file_fragment(fh->fragments.root);
	free(fh);
}

static int splitfs_open(const char *path, struct fuse_file_info *fi)
{
	int fd;
	struct stat buf;
	int ret;
	struct splitfs_file_info *fh;

	if (path[0] != '/') {
		fprintf(stderr, "open called with [%s]\n", path);
		return -ENOENT;
	}

	if ((fi->flags & O_ACCMODE) != O_RDONLY)
		return -EACCES;

	fd = openat(backing_dir_fd, empty_path(path), O_RDONLY);
	if (fd < 0)
		return -errno;

	ret = fstat(fd, &buf);
	if (ret < 0) {
		close(fd);
		return -errno;
	}

	fh = malloc(sizeof(*fh));
	if (fh == NULL) {
		fprintf(stderr, "out of memory\n");
		close(fd);
		return -ENOMEM;
	}

	fh->fd = fd;
	fh->is_fragmented_file = 0;

	if ((buf.st_mode & S_IFMT) == S_IFDIR) {
		ret = is_fragmented_file_dir(fd);
		if (ret < 0) {
			free_splitfs_file_info(fh);
			return ret;
		}

		if (ret) {
			fh->is_fragmented_file = 1;
			INIT_IV_AVL_TREE(&fh->fragments,
					 compare_file_fragments);
			fh->size = 0;

			ret = iterate_fragmented_file_dir(fd, fh,
							  file_frag_handler);
			if (ret < 0) {
				free_splitfs_file_info(fh);
				return ret;
			}
		}
	}

	fi->fh = (int64_t)fh;

	return 0;
}

static struct splitfs_file_fragment *
find_fragment(struct splitfs_file_info *fh, uint64_t offset)
{
	struct iv_avl_node *an;

	an = fh->fragments.root;
	while (an != NULL) {
		struct splitfs_file_fragment *frag;

		frag = iv_container_of(an, struct splitfs_file_fragment, an);
		if (offset < frag->start)
			an = an->left;
		else if (offset < frag->end)
			return frag;
		else
			an = an->right;
	}

	return NULL;
}

static ssize_t xpread(int fd, void *buf, size_t count, off_t offset)
{
	off_t processed;

	processed = 0;
	while (processed < count) {
		ssize_t ret;

		do {
			ret = pread(fd, buf, count - processed, offset);
		} while (ret < 0 && errno == EINTR);

		if (ret <= 0) {
			if (ret < 0)
				perror("pread");
			return processed ? processed : ret;
		}

		buf += ret;
		offset += ret;

		processed += ret;
	}

	return processed;
}

static int splitfs_read(const char *path, char *buf, size_t size,
			off_t offset, struct fuse_file_info *fi)
{
	struct splitfs_file_info *fh = (void *)fi->fh;
	ssize_t processed;

	if (!fh->is_fragmented_file) {
		ssize_t ret;

		ret = pread(fh->fd, buf, size, offset);
		if (ret < 0)
			return -errno;

		return ret;
	}

	if (offset < 0)
		return -EINVAL;
	if (offset >= fh->size)
		return 0;

	if (size > SSIZE_MAX)
		size = SSIZE_MAX;
	if (offset + size > fh->size)
		size = fh->size - offset;

	processed = 0;
	while (size) {
		struct splitfs_file_fragment *frag;
		uint64_t chunk_offset;
		ssize_t chunk_toread;
		char name[32];
		int fd;
		ssize_t ret;

		frag = find_fragment(fh, offset);
		if (frag == NULL)
			goto eio;

		chunk_offset = offset - frag->start;

		chunk_toread = frag->end - offset;
		if (chunk_toread > size)
			chunk_toread = size;

		snprintf(name, sizeof(name), "%.16jx", (intmax_t)frag->start);

		fd = openat(fh->fd, name, O_RDONLY);
		if (fd < 0) {
			perror("openat");
			goto eio;
		}

		ret = xpread(fd, buf, chunk_toread, chunk_offset);
		if (ret <= 0) {
			close(fd);
			goto eio;
		}

		close(fd);

		buf += ret;
		size -= ret;
		offset += ret;

		processed += ret;
	}

	return processed;

eio:
	return processed ? processed : -EIO;
}

static int splitfs_statfs(const char *path, struct statvfs *buf)
{
	int ret;

	if (strcmp(path, "/")) {
		fprintf(stderr, "statfs called with [%s]\n", path);
		return -ENOENT;
	}

	ret = fstatvfs(backing_dir_fd, buf);
	if (ret < 0)
		return -errno;

	return 0;
}

static int splitfs_release(const char *path, struct fuse_file_info *fi)
{
	struct splitfs_file_info *fh = (void *)fi->fh;

	free_splitfs_file_info(fh);

	return 0;
}

static int splitfs_readdir(const char *path, void *buf,
			   fuse_fill_dir_t filler, off_t offset,
			   struct fuse_file_info *fi,
			   enum fuse_readdir_flags flags)
{
	int fd;
	DIR *dirp;
	int ret;

	if (path[0] != '/') {
		fprintf(stderr, "readdir called with [%s]\n", path);
		return -ENOENT;
	}

	fd = openat(backing_dir_fd, empty_path(path), O_DIRECTORY | O_RDONLY);
	if (fd < 0)
		return -errno;

	dirp = fdopendir(fd);
	if (dirp == NULL) {
		ret = -errno;
		close(fd);
		return ret;
	}

	ret = 0;
	while (1) {
		struct dirent *dent;

		errno = 0;

		dent = readdir(dirp);
		if (dent == NULL) {
			ret = -errno;
			break;
		}

		filler(buf, dent->d_name, NULL, 0, 0);
	}

	closedir(dirp);

	return ret;
}

static struct fuse_operations splitfs_oper = {
	.getattr	= splitfs_getattr,
	.readlink	= splitfs_readlink,
	.truncate	= splitfs_truncate,
	.open		= splitfs_open,
	.read		= splitfs_read,
	.statfs		= splitfs_statfs,
	.release	= splitfs_release,
	.readdir	= splitfs_readdir,
};

static void usage(const char *progname)
{
	fprintf(stderr,
"Usage: %s <backingdir> <mountpoint> [options]\n"
"\n"
"General options:\n"
"         --help            print help\n"
"    -V   --version         print version\n"
"    -h   --hash-algo=x     hash algorithm\n"
"\n", progname);
}

enum {
	KEY_HELP,
	KEY_VERSION,
};

struct splitfs_param {
	char	*backing_dir;
};

static struct fuse_opt opts[] = {
	FUSE_OPT_KEY("--help",		KEY_HELP),
	FUSE_OPT_KEY("-V",		KEY_VERSION),
	FUSE_OPT_KEY("--version",	KEY_VERSION),
	FUSE_OPT_END,
};

static int opt_proc(void *data, const char *arg, int key,
		    struct fuse_args *outargs)
{
	struct splitfs_param *param = data;

	if (key == FUSE_OPT_KEY_NONOPT) {
		if (param->backing_dir == NULL) {
			param->backing_dir = strdup(arg);
			return 0;
		}
		return 1;
	}

	if (key == KEY_HELP) {
		usage(outargs->argv[0]);
		fuse_opt_add_arg(outargs, "-ho");
		fuse_main(outargs->argc, outargs->argv, &splitfs_oper, NULL);
		exit(EXIT_FAILURE);
	}

	if (key == KEY_VERSION) {
		fprintf(stderr, "splitfs version: %s\n", PACKAGE_VERSION);
		fuse_opt_add_arg(outargs, "--version");
		fuse_main(outargs->argc, outargs->argv, &splitfs_oper, NULL);
		exit(EXIT_SUCCESS);
	}

	return 1;
}

int main(int argc, char *argv[])
{
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
	struct splitfs_param param;
	int ret;

	memset(&param, 0, sizeof(param));

	if (fuse_opt_parse(&args, &param, opts, opt_proc) < 0)
		return 1;

	if (param.backing_dir == NULL) {
		fprintf(stderr, "missing backing dir\n");
		fprintf(stderr, "see '%s --help' for usage\n", argv[0]);
		return 1;
	}

	backing_dir_fd = open(param.backing_dir, O_RDONLY | O_DIRECTORY);
	if (backing_dir_fd < 0) {
		perror("open");
		return 1;
	}

	ret = fuse_main(args.argc, args.argv, &splitfs_oper, NULL);

	fuse_opt_free_args(&args);
	free(param.backing_dir);

	return ret;
}
