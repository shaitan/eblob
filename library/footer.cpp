#include "footer.h"

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include <boost/crc.hpp>

#include "blob.h"
#include "crypto/sha512.h"
#include "crc32.h"

#include "measure_points.h"

/*
 * eblob_disk_footer contains csum of one chunk of data.
 * @csum - sha512 of one chunk of the data
 *
 * eblob_disk_footers are kept at the end of the recods. One footer per chunk of the data.
 * Each chunk has fixed size = EBLOB_CSUM_CHUNK_SIZE
 */
struct eblob_disk_footer {
	unsigned int			csum;
} __attribute__ ((packed));

struct eblob_disk_footer_old {
	unsigned char			csum[EBLOB_ID_SIZE];
	uint64_t			offset;
} __attribute__ ((packed));

static inline void eblob_convert_disk_footer_old(struct eblob_disk_footer_old *f)
{
	f->offset = eblob_bswap64(f->offset);
}

static inline int crc32_file(int fd, off_t offset, size_t count, unsigned int *result) {
	char buffer[1024];
	size_t read_size = 1024;
	int err = 0;
	*result = 0;

	while (count) {
		if (count < 1024)
			read_size = count;

		err = __eblob_read_ll(fd, buffer, read_size, offset);
		if (err)
			break;

		*result = crc32_16bytes(buffer, read_size, *result);
		count -= read_size;
		offset += read_size;
	}
	return err;
}

static inline uint64_t eblob_get_footer_offset(struct eblob_write_control *wc) {
	if (wc->flags & BLOB_DISK_CTL_CHUNKED_CSUMS) {
		static const size_t f_size = sizeof(struct eblob_disk_footer);

		/* size of whole record without header */
		const uint64_t size = wc->total_size - sizeof(struct eblob_disk_control) - f_size;
		const uint64_t chunks_count = ((size  - 1) / (EBLOB_CSUM_CHUNK_SIZE + f_size)) + 1;
		return wc->total_size - (chunks_count + 1) * f_size;
	} else {
		return wc->total_size - sizeof(struct eblob_disk_footer_old);
	}
}

static int eblob_chunked_csum(struct eblob_backend *b, struct eblob_key *key, struct eblob_write_control *wc,
                              const uint64_t offset, const uint64_t size,
                              struct eblob_disk_footer **footers, uint64_t *footers_offset, uint64_t *footers_size) {
	int err = 0;
	uint64_t chunk;
	uint64_t first_chunk = offset / EBLOB_CSUM_CHUNK_SIZE;
	uint64_t last_chunk = (offset + size - 1) / EBLOB_CSUM_CHUNK_SIZE + 1;
	static const size_t f_size = sizeof(struct eblob_disk_footer),
	                    hdr_size = sizeof(struct eblob_disk_control);
	const uint64_t footer_offset = eblob_get_footer_offset(wc);
	*footers_offset = wc->ctl_data_offset + footer_offset + first_chunk * f_size;
	*footers_size = f_size * (last_chunk - first_chunk);

	*footers = (struct eblob_disk_footer *)calloc(1, *footers_size);
	if (!*footers) {
		err = -ENOMEM;
		goto err_out_exit;
	}

	for (chunk = first_chunk; chunk < last_chunk; ++chunk) {
		struct eblob_disk_footer *curr_footer = *footers + (chunk - first_chunk);
		const uint64_t data_offset = wc->ctl_data_offset + hdr_size + chunk * EBLOB_CSUM_CHUNK_SIZE;
		const uint64_t offset_max = wc->ctl_data_offset + footer_offset;
		uint64_t data_size = EBLOB_CSUM_CHUNK_SIZE;
		if (data_offset + data_size > offset_max) {
			data_size = offset_max - data_offset;
		}
		err = crc32_file(wc->data_fd, data_offset, data_size, &curr_footer->csum);
		//eblob_log(b->cfg.log, EBLOB_LOG_DEBUG, "blob: %s: calculated csum: %x: fd: %d, "
		//          "data_offset: %" PRIu64 ", data_size: %" PRIu64 ", err: %d\n",
		//          eblob_dump_id(key->id), curr_footer->csum, wc->data_fd, data_offset, data_size, err);
		if (err)
			goto err_out_exit;
	}

	return 0;

err_out_exit:
	free(*footers);
	*footers = NULL;
	return err;
}

uint64_t eblob_calculate_footer_size(struct eblob_backend *b, uint64_t data_size) {
	if (b->cfg.blob_flags & EBLOB_NO_FOOTER)
		return 0;
	if (data_size == 0)
		return 0;

	const uint64_t footers_count = (data_size - 1) / EBLOB_CSUM_CHUNK_SIZE + 2;

	return footers_count * sizeof(struct eblob_disk_footer);
}

int eblob_write_commit_footer(struct eblob_backend *b, struct eblob_key *key,
                              struct eblob_write_control *wc) {
	if (b->cfg.blob_flags & EBLOB_NO_FOOTER)
		return 0;
	if (wc->flags & BLOB_DISK_CTL_NOCSUM)
		return 0;

	int err = 0;
	struct eblob_disk_footer f;
	const uint64_t footer_offset = eblob_get_footer_offset(wc);
	const uint64_t total_footers_size = wc->total_size - footer_offset - sizeof(f);
	const uint64_t final_footer_offset = wc->ctl_data_offset + footer_offset + total_footers_size;

	err = crc32_file(wc->data_fd, wc->ctl_data_offset + footer_offset, total_footers_size, &f.csum);
	if (err)
		goto err_out_exit;

	err = __eblob_write_ll(wc->data_fd, &f, sizeof(f), final_footer_offset);
	eblob_log(b->cfg.log, EBLOB_LOG_DEBUG, "blob: %s: updated final footer: footers_offset: %" PRIu64 ", footers_size: %" PRIu64 ", err: %d\n",
	          eblob_dump_id(key->id), final_footer_offset, sizeof(f), err);
	if (err != 0)
		goto err_out_exit;

err_out_exit:
	return err;
}

static int eblob_csum_ok_old(struct eblob_backend *b, struct eblob_key *key, struct eblob_write_control *wc) {
	struct eblob_disk_footer_old f;
	unsigned char csum[EBLOB_ID_SIZE];
	int err = 0;
	off_t off = wc->ctl_data_offset + wc->total_size - sizeof(struct eblob_disk_footer_old);

	err = __eblob_read_ll(wc->data_fd, &f, sizeof(struct eblob_disk_footer), off);
	if (err)
		goto err_out_exit;

	memset(csum, 0, sizeof(csum));
	/* zero-filled csum is ok csum */
	if (!memcmp(csum, f.csum, sizeof(f.csum)))
		goto err_out_exit;

	off = wc->ctl_data_offset + sizeof(struct eblob_disk_control);
	err = sha512_file(wc->data_fd, off, wc->total_data_size, csum);
	if (err)
		goto err_out_exit;

	if (memcmp(csum, f.csum, sizeof(csum))) {
		err = -EILSEQ;
		goto err_out_exit;
	}
err_out_exit:
	eblob_log(b->cfg.log, EBLOB_LOG_DEBUG, "blob: %s: eblob_csum_ok_old: err: %d\n",
	          eblob_dump_id(key->id), err);
	return err;
}

static int eblob_csum_ok_new(struct eblob_backend *b, struct eblob_key *key, struct eblob_write_control *wc) {
	int err = 0;
	struct eblob_disk_footer *calc_footers = NULL,
	                         *check_footers = NULL;
	uint64_t footers_offset = 0,
	         footers_size = 0;

	err = eblob_chunked_csum(b, key, wc, wc->offset, wc->size, &calc_footers, &footers_offset, &footers_size);
	if (err)
		goto err_out_exit;

	check_footers = (struct eblob_disk_footer *)calloc(1, footers_size);
	if (!check_footers) {
		err = -ENOMEM;
		goto err_free_calc;
	}

	err = __eblob_read_ll(wc->data_fd, check_footers, footers_size, footers_offset);
	if (err) {
		eblob_log(b->cfg.log, EBLOB_LOG_ERROR, "blob: failed to read footer: fd: %d, size: %"PRIu64
		          ", offset: %" PRIu64 "\n", wc->data_fd, footers_size, footers_offset);
		goto err_free_check;
	}

	if (memcmp(calc_footers, check_footers, footers_size)) {
		uint64_t footers_count = footers_size / sizeof(struct eblob_disk_footer);
		eblob_log(b->cfg.log, EBLOB_LOG_DEBUG, "blob: failed to check checksum: footers_size: %" PRIu64
		          ", footers_count: %" PRIu64"\n", footers_size, footers_count);
		//for (uint64_t i = 0; i < footers_count; ++i) {
		//	eblob_log(b->cfg.log, EBLOB_LOG_DEBUG, "checking csum: %x\n", check_footers[i].csum);
		//}
		err = -EILSEQ;
	}

err_free_check:
	free(check_footers);
err_free_calc:
	free(calc_footers);
err_out_exit:
	eblob_log(b->cfg.log, EBLOB_LOG_DEBUG, "blob: %s: eblob_csum_ok_new: err: %d\n",
	          eblob_dump_id(key->id), err);
	return err;
}

/**
 * eblob_csum_ok() - verifies checksum of entry pointed by @wc.
 */
int eblob_check_csum(struct eblob_backend *b, struct eblob_key *key, struct eblob_write_control *wc) {
	if (b->cfg.blob_flags & EBLOB_NO_FOOTER ||
	    wc->flags & BLOB_DISK_CTL_NOCSUM)
		return 0;

	int err = 0;

	FORMATTED(HANDY_TIMER_SCOPE, ("eblob.%u.csum.ok", b->cfg.stat_id));

	if (wc->total_size < wc->total_data_size) {
		err = -EINVAL;
		goto err_out_exit;
	}

	if (wc->flags & BLOB_DISK_CTL_CHUNKED_CSUMS)
		err = eblob_csum_ok_new(b, key, wc);
	else
		err = eblob_csum_ok_old(b, key, wc);

err_out_exit:
	return err;
}

int eblob_commit_footer(struct eblob_backend *b, struct eblob_key *key, struct eblob_write_control *wc) {
	if (b->cfg.blob_flags & EBLOB_NO_FOOTER ||
	    wc->flags & BLOB_DISK_CTL_NOCSUM)
		return 0;

	FORMATTED(HANDY_TIMER_SCOPE, ("eblob.%u.commit_footer", b->cfg.stat_id));

	int err = 0;
	struct eblob_disk_footer *footers = NULL;
	struct eblob_disk_footer final_footer;
	uint64_t footers_offset = 0, footers_size = 0;

	err = eblob_chunked_csum(b, key, wc, 0, wc->total_data_size, &footers, &footers_offset, &footers_size);
	if (err)
		goto err_out_exit;

	final_footer.csum = crc32_16bytes(footers, footers_size);

	err = __eblob_write_ll(wc->data_fd, footers, footers_size, footers_offset);
	if (err)
		goto err_out_exit;

	footers_offset += footers_size;

	err = __eblob_write_ll(wc->data_fd, &final_footer, sizeof(struct eblob_disk_footer), footers_offset);
	if (err)
		goto err_out_exit;


err_out_exit:
	free(footers);
	return err;
}
