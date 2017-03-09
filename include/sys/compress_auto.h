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


#ifndef _SYS_AUTO_COMPRESS_H
#define _SYS_AUTO_COMPRESS_H

#define COMPRESS_AUTO_LEVELS 2

#include <sys/spa.h>
#include <sys/zio.h>


size_t compress_auto(zio_t *zio,enum zio_compress *c, abd_t *src, void *dst, size_t s_len);
void compress_auto_calc_avg_nozero(uint64_t act, uint64_t *res, int n);

#endif /* _SYS_AUTO_COMPRESS_H */
