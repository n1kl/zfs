/*
 * CDDL HEADER START
 *
 * The contents of this file are subject to the terms of the
 * Common Development and Distribution License (the "License").
 * You may not use this file except in compliance with the License.
 *
 * You can obtain a copy of the license at usr/src/OPENSOLARIS.LICENSE
 * or http://www.opensolaris.org/os/licensing.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * When distributing Covered Code, include this CDDL HEADER in each
 * file and include the License file at usr/src/OPENSOLARIS.LICENSE.
 * If applicable, add the following below this CDDL HEADER, with the
 * fields enclosed by brackets "[]" replaced with your own identifying
 * information: Portions Copyright [yyyy] [name of copyright owner]
 *
 * CDDL HEADER END
 */

/*
 * Copyright 2009 Sun Microsystems, Inc.  All rights reserved.
 * Use is subject to license terms.
 */
/*
 * Copyright (c) 2013 by Saso Kiselkov. All rights reserved.
 */

/*
 * Copyright (c) 2013, 2016 by Delphix. All rights reserved.
 */

#include <sys/zfs_context.h>
#include <sys/compress.h>
#include <sys/spa.h>
#include <sys/zfeature.h>
#include <sys/zio.h>
#include <sys/zio_compress.h>

/*
 * Compression vectors.
 */
zio_compress_info_t zio_compress_table[ZIO_COMPRESS_META_FUNCTIONS] = {
	{"inherit",		0,	NULL,		NULL, ZIO_COMPRESS_INHERIT},
	{"on",			0,	NULL,		NULL, ZIO_COMPRESS_ON},
	{"uncompressed",	0,	NULL,		NULL, ZIO_COMPRESS_OFF},
	{"lzjb",		0,	lzjb_compress,	lzjb_decompress, ZIO_COMPRESS_LZJB},
	{"empty",		0,	NULL,		NULL, ZIO_COMPRESS_EMPTY},
	{"gzip-1",		1,	gzip_compress,	gzip_decompress, ZIO_COMPRESS_GZIP_1},
	{"gzip-2",		2,	gzip_compress,	gzip_decompress, ZIO_COMPRESS_GZIP_2},
	{"gzip-3",		3,	gzip_compress,	gzip_decompress, ZIO_COMPRESS_GZIP_3},
	{"gzip-4",		4,	gzip_compress,	gzip_decompress, ZIO_COMPRESS_GZIP_4},
	{"gzip-5",		5,	gzip_compress,	gzip_decompress, ZIO_COMPRESS_GZIP_5},
	{"gzip-6",		6,	gzip_compress,	gzip_decompress, ZIO_COMPRESS_GZIP_6},
	{"gzip-7",		7,	gzip_compress,	gzip_decompress, ZIO_COMPRESS_GZIP_7},
	{"gzip-8",		8,	gzip_compress,	gzip_decompress, ZIO_COMPRESS_GZIP_8},
	{"gzip-9",		9,	gzip_compress,	gzip_decompress, ZIO_COMPRESS_GZIP_9},
	{"zle",			64,	zle_compress,	zle_decompress, ZIO_COMPRESS_ZLE},
	{"lz4",			0,	lz4_compress_zfs, lz4_decompress_zfs, ZIO_COMPRESS_LZ4},
	{"compressfunctionsplaceholder", 0, NULL, NULL, ZIO_COMPRESS_FUNCTIONS},
	{"lz4fast-1",	1,	lz4_compress_zfs,	lz4_decompress_zfs, ZIO_COMPRESS_LZ4},
	{"lz4fast-2",	2,	lz4_compress_zfs,	lz4_decompress_zfs,	ZIO_COMPRESS_LZ4},
	{"lz4fast-3",	3,	lz4_compress_zfs,	lz4_decompress_zfs,	ZIO_COMPRESS_LZ4},
	{"lz4fast-4",	4,	lz4_compress_zfs,	lz4_decompress_zfs, ZIO_COMPRESS_LZ4},
	{"lz4fast-5",	5,	lz4_compress_zfs,	lz4_decompress_zfs, ZIO_COMPRESS_LZ4},
	{"lz4fast-6",	6,	lz4_compress_zfs,	lz4_decompress_zfs, ZIO_COMPRESS_LZ4},
	{"lz4fast-7",	7,	lz4_compress_zfs,	lz4_decompress_zfs, ZIO_COMPRESS_LZ4},
	{"lz4fast-8",	8,	lz4_compress_zfs,	lz4_decompress_zfs, ZIO_COMPRESS_LZ4},
	{"lz4fast-9",	9,	lz4_compress_zfs,	lz4_decompress_zfs, ZIO_COMPRESS_LZ4},
	{"lz4fast-10",	10,	lz4_compress_zfs,	lz4_decompress_zfs, ZIO_COMPRESS_LZ4},
	{"lz4fast-11",	11,	lz4_compress_zfs,	lz4_decompress_zfs, ZIO_COMPRESS_LZ4},
	{"lz4fast-12",	12,	lz4_compress_zfs,	lz4_decompress_zfs, ZIO_COMPRESS_LZ4},
	{"lz4fast-13",	13,	lz4_compress_zfs,	lz4_decompress_zfs, ZIO_COMPRESS_LZ4},
	{"lz4fast-14",	14,	lz4_compress_zfs,	lz4_decompress_zfs, ZIO_COMPRESS_LZ4},
	{"lz4fast-15",	15,	lz4_compress_zfs,	lz4_decompress_zfs, ZIO_COMPRESS_LZ4},
	{"lz4fast-16",	16,	lz4_compress_zfs,	lz4_decompress_zfs, ZIO_COMPRESS_LZ4},
	{"lz4fast-17",	17,	lz4_compress_zfs,	lz4_decompress_zfs, ZIO_COMPRESS_LZ4},
	{"lz4fast-18",	18,	lz4_compress_zfs,	lz4_decompress_zfs, ZIO_COMPRESS_LZ4},
	{"lz4fast-19",	19,	lz4_compress_zfs,	lz4_decompress_zfs, ZIO_COMPRESS_LZ4},
	{"lz4fast-20",	20,	lz4_compress_zfs,	lz4_decompress_zfs, ZIO_COMPRESS_LZ4},
	{"lz4fast-30",	30,	lz4_compress_zfs,	lz4_decompress_zfs, ZIO_COMPRESS_LZ4},
	{"lz4fast-40",	40,	lz4_compress_zfs,	lz4_decompress_zfs, ZIO_COMPRESS_LZ4},
	{"lz4fast-50",	50,	lz4_compress_zfs,	lz4_decompress_zfs, ZIO_COMPRESS_LZ4},
	{"lz4fast-60",	60,	lz4_compress_zfs,	lz4_decompress_zfs, ZIO_COMPRESS_LZ4},
	{"lz4fast-70",	70,	lz4_compress_zfs,	lz4_decompress_zfs, ZIO_COMPRESS_LZ4},
	{"lz4fast-80",	80,	lz4_compress_zfs,	lz4_decompress_zfs, ZIO_COMPRESS_LZ4},
	{"lz4fast-90",	90,	lz4_compress_zfs,	lz4_decompress_zfs, ZIO_COMPRESS_LZ4},
	{"lz4fast-100",	100,lz4_compress_zfs,	lz4_decompress_zfs, ZIO_COMPRESS_LZ4},
	{"auto",		0,	NULL,		NULL,	ZIO_COMPRESS_AUTO},
	{"qos-10",		0,	NULL,		NULL,	ZIO_COMPRESS_QOS_10},
	{"qos-20",		0,	NULL,		NULL,	ZIO_COMPRESS_QOS_20},
	{"qos-30",		0,	NULL,		NULL,	ZIO_COMPRESS_QOS_30},
	{"qos-40",		0,	NULL,		NULL,	ZIO_COMPRESS_QOS_40},
	{"qos-50",		0,	NULL,		NULL,	ZIO_COMPRESS_QOS_50},
	{"qos-100",		0,	NULL,		NULL,	ZIO_COMPRESS_QOS_100},
	{"qos-150",		0,	NULL,		NULL,	ZIO_COMPRESS_QOS_150},
	{"qos-200",		0,	NULL,		NULL,	ZIO_COMPRESS_QOS_200},
	{"qos-250",		0,	NULL,		NULL,	ZIO_COMPRESS_QOS_250},
	{"qos-300",		0,	NULL,		NULL,	ZIO_COMPRESS_QOS_300},
	{"qos-350",		0,	NULL,		NULL,	ZIO_COMPRESS_QOS_350},
	{"qos-400",		0,	NULL,		NULL,	ZIO_COMPRESS_QOS_400},
	{"qos-450",		0,	NULL,		NULL,	ZIO_COMPRESS_QOS_450},
	{"qos-500",		0,	NULL,		NULL,	ZIO_COMPRESS_QOS_500},
	{"qos-550",		0,	NULL,		NULL,	ZIO_COMPRESS_QOS_550},
	{"qos-600",		0,	NULL,		NULL,	ZIO_COMPRESS_QOS_600},
	{"qos-650",		0,	NULL,		NULL,	ZIO_COMPRESS_QOS_650},
	{"qos-700",		0,	NULL,		NULL,	ZIO_COMPRESS_QOS_700},
	{"qos-750",		0,	NULL,		NULL,	ZIO_COMPRESS_QOS_750},
	{"qos-800",		0,	NULL,		NULL,	ZIO_COMPRESS_QOS_800},
	{"qos-850",		0,	NULL,		NULL,	ZIO_COMPRESS_QOS_850},
	{"qos-900",		0,	NULL,		NULL,	ZIO_COMPRESS_QOS_900},
	{"qos-950",		0,	NULL,		NULL,	ZIO_COMPRESS_QOS_950},
	{"qos-1000",		0,	NULL,		NULL,	ZIO_COMPRESS_QOS_1000}
};

enum zio_compress
zio_compress_select(spa_t *spa, enum zio_compress child,
    enum zio_compress parent)
{
	enum zio_compress result;

	ASSERT(child < ZIO_COMPRESS_META_FUNCTIONS && child != ZIO_COMPRESS_FUNCTIONS);
	ASSERT(parent < ZIO_COMPRESS_META_FUNCTIONS && parent != ZIO_COMPRESS_FUNCTIONS);
	ASSERT(parent != ZIO_COMPRESS_INHERIT);

	result = child;
	if (result == ZIO_COMPRESS_INHERIT)
		result = parent;

	if (result == ZIO_COMPRESS_ON) {
		if (spa_feature_is_active(spa, SPA_FEATURE_LZ4_COMPRESS))
			result = ZIO_COMPRESS_LZ4_ON_VALUE;
		else
			result = ZIO_COMPRESS_LEGACY_ON_VALUE;
	}

	return (result);
}

/*ARGSUSED*/
static int
zio_compress_zeroed_cb(void *data, size_t len, void *private)
{
	uint64_t *end = (uint64_t *)((char *)data + len);
	uint64_t *word;

	for (word = data; word < end; word++)
		if (*word != 0)
			return (1);

	return (0);
}

size_t
zio_compress_data(enum zio_compress c, abd_t *src, void *dst, size_t s_len)
{
	size_t c_len, d_len;
	zio_compress_info_t *ci = &zio_compress_table[c];
	void *tmp;

	ASSERT((uint_t)c < ZIO_COMPRESS_META_FUNCTIONS && (uint_t)c != ZIO_COMPRESS_FUNCTIONS);
	ASSERT((uint_t)c == ZIO_COMPRESS_EMPTY || ci->ci_compress != NULL);

	/*
	 * If the data is all zeroes, we don't even need to allocate
	 * a block for it.  We indicate this by returning zero size.
	 */
	if (abd_iterate_func(src, 0, s_len, zio_compress_zeroed_cb, NULL) == 0)
		return (0);

	if (c == ZIO_COMPRESS_EMPTY)
		return (s_len);

	/* Compress at least 12.5% */
	d_len = s_len - (s_len >> 3);

	/* No compression algorithms can read from ABDs directly */
	tmp = abd_borrow_buf_copy(src, s_len);
	c_len = ci->ci_compress(tmp, dst, s_len, d_len, ci->ci_level);
	abd_return_buf(src, tmp, s_len);

	if (c_len > d_len)
		return (s_len);

	ASSERT3U(c_len, <=, d_len);
	return (c_len);
}

int
zio_decompress_data_buf(enum zio_compress c, void *src, void *dst,
    size_t s_len, size_t d_len)
{
	zio_compress_info_t *ci = &zio_compress_table[c];
	if ((uint_t)c >= ZIO_COMPRESS_FUNCTIONS || ci->ci_decompress == NULL)
		return (SET_ERROR(EINVAL));

	return (ci->ci_decompress(src, dst, s_len, d_len, ci->ci_level));
}

int
zio_decompress_data(enum zio_compress c, abd_t *src, void *dst,
    size_t s_len, size_t d_len)
{
	void *tmp = abd_borrow_buf_copy(src, s_len);
	int ret = zio_decompress_data_buf(c, tmp, dst, s_len, d_len);
	abd_return_buf(src, tmp, s_len);

	return (ret);
}
