/*
 * Copyright (C) 2015 Lennert Buytenhek
 *
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License version
 * 2.1 as published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License version 2.1 for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License version 2.1 along with this library; if not, write to the
 * Free Software Foundation, Inc., 51 Franklin Street - Fifth Floor,
 * Boston, MA 02110-1301, USA.
 */

#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <pthread.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>
#include "common.h"

ssize_t xpread(int fd, void *buf, size_t count, off_t offset)
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

ssize_t xpwrite(int fd, const void *buf, size_t count, off_t offset)
{
	off_t processed;

	processed = 0;
	while (processed < count) {
		ssize_t ret;

		do {
			ret = pwrite(fd, buf, count - processed, offset);
		} while (ret < 0 && errno == EINTR);

		if (ret < 0) {
			perror("pwrite");
			exit(1);
		}

		buf += ret;
		offset += ret;

		processed += ret;
	}

	return processed;
}

void xsem_post(sem_t *sem)
{
	if (sem_post(sem) < 0) {
		perror("sem_post");
		exit(1);
	}
}

void xsem_wait(sem_t *sem)
{
	if (sem_wait(sem) < 0) {
		perror("sem_wait");
		exit(1);
	}
}

static void xsem_init(sem_t *sem, int pshared, unsigned int value)
{
	if (sem_init(sem, pshared, value) < 0) {
		perror("sem_init");
		exit(1);
	}
}

static void xpthread_create(pthread_t *thread, const pthread_attr_t *attr,
			    void *(*start_routine)(void *), void *arg)
{
	int ret;

	ret = pthread_create(thread, attr, start_routine, arg);
	if (ret) {
		fprintf(stderr, "pthread_create: %s\n", strerror(ret));
		exit(1);
	}
}

static void xpthread_join(pthread_t thread, void **retval)
{
	int ret;

	ret = pthread_join(thread, retval);
	if (ret) {
		fprintf(stderr, "pthread_join: %s\n", strerror(ret));
		exit(1);
	}
}

static void xsem_destroy(sem_t *sem)
{
	if (sem_destroy(sem) < 0) {
		perror("sem_destroy");
		exit(1);
	}
}

void run_threads(void *(*handler)(void *), void *cookie)
{
	int nthreads;
	struct worker_thread *wt;
	pthread_t *tid;
	int i;

	nthreads = 4 * sysconf(_SC_NPROCESSORS_ONLN);

	wt = alloca(nthreads * sizeof(*wt));
	tid = alloca(nthreads * sizeof(*tid));

	for (i = 0; i < nthreads; i++) {
		xsem_init(&wt[i].sem0, 0, 0);
		xsem_init(&wt[i].sem1, 0, 0);
		wt[i].next = &wt[(i + 1) % nthreads];
		wt[i].cookie = cookie;
	}

	for (i = 0; i < nthreads; i++)
		xpthread_create(tid + i, NULL, handler, wt + i);

	xsem_post(&wt[0].sem0);
	xsem_post(&wt[0].sem1);

	for (i = 0; i < nthreads; i++)
		xpthread_join(tid[i], NULL);

	for (i = 0; i < nthreads; i++) {
		xsem_destroy(&wt[i].sem0);
		xsem_destroy(&wt[i].sem1);
	}
}
