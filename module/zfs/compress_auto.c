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

#include <sys/compress_auto.h>
#include <sys/zio_compress.h>
#include <sys/spa_impl.h>
#include <sys/vdev_impl.h>


enum zio_compress ac_compress[COMPRESS_AUTO_LEVELS] = {
	ZIO_COMPRESS_LZ4,
	ZIO_COMPRESS_GZIP_1,
};


void
compress_auto_calc_avg_nozero(uint64_t act, uint64_t *res, int n) {
	uint64_t prev = *res;
	if (act) {
		if (prev) {
			*res = (act + prev * (n - 1)) / n;
		} else {
			*res = act;
		}
	}
}


uint64_t compress_auto_min_queue_delay(vdev_t *vd, uint64_t offset) {

	uint64_t min_time = 0;

	if(!vd->vdev_children) { //is leaf
		uint64_t vd_queued_size_write = vd->vdev_queue.vq_class[ZIO_PRIORITY_ASYNC_WRITE].vqc_queued_size;
		uint64_t vd_writespeed = vd->vdev_stat_ex.vsx_diskBps[ZIO_TYPE_WRITE];

		if (vd_queued_size_write >= offset * 100 ) { //keep 100 zios in queue
			vd_queued_size_write -= offset * 100;
		} else {
			vd_queued_size_write = 0;
		}

		uint64_t vd_info_active_write = vd->vdev_queue.vq_class[ZIO_PRIORITY_ASYNC_WRITE].vqc_active;
		uint64_t vd_info_waiting_write = avl_numnodes(&vd->vdev_queue.vq_class[ZIO_PRIORITY_ASYNC_WRITE].vqc_queued_tree);
		vdev_stat_ex_t *vsx = &vd->vdev_stat_ex;
		uint64_t io_exp_queue_time;
		uint64_t trans = 1000l*1000l*1000l;
		hrtime_t io_delta = 0;
		hrtime_t io_delay = 0;
		hrtime_t io_queue_delay = 0;

		//MTAC print
		io_exp_queue_time = (trans*vd_queued_size_write)/vsx->vsx_diskBps[ZIO_TYPE_WRITE];
		dprintf(",MTACVD:, [	vdev;ts;txg;act_w;wait_w;queuedel;diskdel;diskspeed;exp_queuedel;queued_size;disk_speed_avg],"
				"%p,%llu,%llu,%llu,	%d,%llu,%llu,%d,%llu,%llu,%llu,%d",
				vd, gethrtime(), 0, vd_info_active_write, vd_info_waiting_write,	io_queue_delay,	io_delay,	0,	io_exp_queue_time,	vd_queued_size_write,	vsx->vsx_diskBps[ZIO_TYPE_WRITE], 0);




		if (vd_writespeed) {
			//uint64_t trans = 1000 * 1000 * 1000;
			return (vd_queued_size_write * trans) / vd_writespeed;
		}
		return 0;
	} else {
		int i;
		for(i=0; i < vd->vdev_children; i++)
		{
			uint64_t time = compress_auto_min_queue_delay(vd->vdev_child[i], offset);
			if (time) {
				if(min_time == 0) {
					min_time = time;
				} else if(time < min_time) {
					min_time = time;
				}
			}
		}
	}
	return min_time;
}


void compress_auto_update(zio_t *zio)
{
	zio_t *pio = zio->io_temp_parent;
	if (pio) {
		if (zio->io_compress_auto_delay) {
			int n = 10;
			uint64_t trans = 1000 * 1000 * 1000;
			uint64_t compressBps = (zio->io_lsize * trans) / zio->io_compress_auto_delay;
			compress_auto_calc_avg_nozero(compressBps, &pio->io_compress_auto_Bps[zio->io_compress_level], n);
		}

		if (zio->io_compress_auto_exploring) {
			pio->io_compress_auto_exploring = B_FALSE;
			zio->io_compress_auto_exploring = B_FALSE;
		} else {
			pio->io_compress_level = zio->io_compress_level;
		}

		//MTAC print
				dprintf(",MTACC:, [ts; compressdel; level],%llu,%llu,%d",gethrtime(),zio->io_compress_auto_delay,zio->io_compress_level);

	}
}


size_t
compress_auto(zio_t *zio, enum zio_compress *c, abd_t *src, void *dst, size_t s_len)
{
	hrtime_t ac_compress_begin = gethrtime();
	size_t psize;
	spa_t *spa = zio->io_spa;
	vdev_t *rvd = spa->spa_root_vdev;

	zio_t *pio = zio_unique_parent(zio);
	uint64_t exp_queue_delay = compress_auto_min_queue_delay(rvd,zio->io_lsize);

	zio->io_temp_parent = pio;

	*c = ac_compress[0];

	int level = 0;
	if (pio) {
		level = pio->io_compress_level;

		if (pio->io_compress_auto_Bps[level]) {
			uint64_t trans = 1000 * 1000 * 1000;

			if ((zio->io_lsize * trans)/pio->io_compress_auto_Bps[level] < exp_queue_delay) {
				if (level < COMPRESS_AUTO_LEVELS - 1) {
					if (pio->io_compress_auto_Bps[level + 1]) {
						if ((zio->io_lsize * trans) / pio->io_compress_auto_Bps[level + 1] < exp_queue_delay) {
							level++;
						} //else stay on level
					} else if (!pio->io_compress_auto_exploring) {
							pio->io_compress_auto_exploring = B_TRUE;
							zio->io_compress_auto_exploring = B_TRUE;
							level++;
					} //else stay on level
				}
			} else {
				while ((zio->io_lsize * trans)/pio->io_compress_auto_Bps[level] > exp_queue_delay){
					if (level > 0) {
						level--;
					} else {
						break;
					}
				}
			} //else stay on level
		}

		*c = ac_compress[level];
		zio->io_compress_level = level;
	}

	psize = zio_compress_data(*c, src, dst, s_len);
	zio->io_compress_auto_delay = gethrtime() - ac_compress_begin;

	dprintf(",MTACAC:, [ts;exp_avg;init_compress; compress_avgdur; compressBps; compress;level],%llu,%llu,%d,%llu,%llu,%d,%d",gethrtime(),exp_queue_delay,pio->io_compress_level,pio->io_compress_auto_Bps[level] ? (zio->io_lsize*1000l*1000l*1000l)/pio->io_compress_auto_Bps[level] : 0,pio->io_compress_auto_Bps[level],*c,level);


	compress_auto_update(zio);



	return psize;
}
