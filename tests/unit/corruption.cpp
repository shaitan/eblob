#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE CORRUPTION library test

#include <boost/test/unit_test.hpp>
#include <boost/filesystem.hpp>
#include <library/blob.h>

#include "library/crypto/sha512.h"

#include "eblob/eblob.hpp"

class eblob_wrapper {
public:
	eblob_wrapper()
	: data_dir_template_("/tmp/eblob-test-XXXXXX")
	, data_dir_{mkdtemp(&data_dir_template_.front())}
	, data_path_{data_dir_ + "/data"}
	, log_path_{data_dir_ + "/log.log"}
	, logger_{log_path_.c_str(), EBLOB_LOG_DEBUG}
	, backend_{nullptr} {
		restart();
	}

	void restart() {
		stop();
		backend_ = [&]() {
			eblob_config config;
			memset(&config, 0, sizeof(config));
			config.blob_flags = EBLOB_L2HASH | EBLOB_DISABLE_THREADS | EBLOB_AUTO_INDEXSORT;
			config.sync = -2;
			config.log = logger_.log();
			config.file = (char *)data_path_.c_str();
			config.blob_size = EBLOB_BLOB_DEFAULT_BLOB_SIZE;
			config.records_in_blob = EBLOB_BLOB_DEFAULT_RECORDS_IN_BLOB;
			config.defrag_percentage = EBLOB_DEFAULT_DEFRAG_PERCENTAGE;
			config.defrag_timeout = EBLOB_DEFAULT_DEFRAG_TIMEOUT;
			config.index_block_size = EBLOB_INDEX_DEFAULT_BLOCK_SIZE;
			config.index_block_bloom_length = EBLOB_INDEX_DEFAULT_BLOCK_BLOOM_LENGTH;
			config.blob_size_limit = 0;
			config.defrag_time = EBLOB_DEFAULT_DEFRAG_TIME;
			config.defrag_splay = EBLOB_DEFAULT_DEFRAG_SPLAY;
			config.periodic_timeout = EBLOB_DEFAULT_PERIODIC_THREAD_TIMEOUT;
			config.stat_id = 12345;
			config.chunks_dir = nullptr;
			return eblob_init(&config);
		}();
	}

	void stop() {
		if (backend_) {
			eblob_cleanup(backend_);
			backend_ = nullptr;
		}
	}

	~eblob_wrapper() {
		stop();
		boost::filesystem::remove_all(data_dir_);
	}

	eblob_backend *get() { return backend_; }

private:
	std::string data_dir_template_;
	const std::string data_dir_;
	const std::string data_path_;
	const std::string log_path_;
	ioremap::eblob::eblob_logger logger_;
	eblob_backend *backend_;
};

eblob_key hash(std::string key) {
	eblob_key ret;
	sha512_buffer(key.data(), key.size(), ret.id);
	return ret;
}

BOOST_AUTO_TEST_CASE(test_header_corruption) {
	/* corrupt record's header in blob and check that record isn't considered as corrupted (since data is correct)
	 * and read is failed with -EINVAL
	 */
	eblob_wrapper wrapper;
	BOOST_REQUIRE(wrapper.get() != nullptr);

	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 0);

	auto key = hash("some key");
	constexpr char data[] = "some data";
	constexpr uint64_t MY_DISK_CTL_FLAG = 1 << 30;

	eblob_write_control wc;
	BOOST_REQUIRE_EQUAL(
		eblob_write_return(wrapper.get(), &key, (void *)data, /*offset*/ 0, sizeof(data), MY_DISK_CTL_FLAG, &wc),
		0);
	BOOST_REQUIRE_EQUAL(wc.flags, MY_DISK_CTL_FLAG | BLOB_DISK_CTL_CHUNKED_CSUM);

	// read current byte
	char current_byte;
	BOOST_REQUIRE_EQUAL(__eblob_read_ll(wc.data_fd, &current_byte, sizeof(current_byte), 0), 0);
	// corrupt header
	BOOST_REQUIRE_EQUAL(__eblob_write_ll(wc.data_fd, "a", 1, 0), 0);

	// checksum verification should not failed
	BOOST_REQUIRE_EQUAL(eblob_verify_checksum(wrapper.get(), &key, &wc), 0);
	// verify checksum via read
	BOOST_REQUIRE_EQUAL(eblob_read_return(wrapper.get(), &key, EBLOB_READ_CSUM, &wc), -EINVAL);
	// read @wc without checksum verification
	BOOST_REQUIRE_EQUAL(eblob_read_return(wrapper.get(), &key, EBLOB_READ_NOCSUM, &wc), -EINVAL);
	// check that the record was not marked corrupted
	BOOST_REQUIRE_EQUAL(wc.flags, MY_DISK_CTL_FLAG | BLOB_DISK_CTL_CHUNKED_CSUM);
	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 0);

	// restore header
	BOOST_REQUIRE_EQUAL(__eblob_write_ll(wc.data_fd, &current_byte, sizeof(current_byte), 0), 0);
	// checksum verification should succeed
	BOOST_REQUIRE_EQUAL(eblob_verify_checksum(wrapper.get(), &key, &wc), 0);
	// read @wc with checksum verification should succeed
	BOOST_REQUIRE_EQUAL(eblob_read_return(wrapper.get(), &key, EBLOB_READ_CSUM, &wc), 0);
	// the record should still be marked corrupted
	BOOST_REQUIRE_EQUAL(wc.flags, MY_DISK_CTL_FLAG | BLOB_DISK_CTL_CHUNKED_CSUM);

	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 0);

	wrapper.restart();

	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 0);
	BOOST_REQUIRE_EQUAL(eblob_remove(wrapper.get(), &key), 0);
	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 0);
}

BOOST_AUTO_TEST_CASE(test_data_corruption) {
	/* corrupt record's data and check that the record is considered as corrupted */
	eblob_wrapper wrapper;
	BOOST_REQUIRE(wrapper.get() != nullptr);

	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 0);

	auto key = hash("some key");
	constexpr char data[] = "some data";
	constexpr uint64_t MY_DISK_CTL_FLAG = 1 << 30;

	eblob_write_control wc;
	BOOST_REQUIRE_EQUAL(
		eblob_write_return(wrapper.get(), &key, (void *)data, /*offset*/ 0, sizeof(data), MY_DISK_CTL_FLAG, &wc),
		0);
	BOOST_REQUIRE_EQUAL(wc.index, 0); // in the first blob
	BOOST_REQUIRE_EQUAL(wc.ctl_index_offset, 0); // at the beginning of the index file
	BOOST_REQUIRE_EQUAL(wc.ctl_data_offset, 0); // at the beginning of the blob
	BOOST_REQUIRE_EQUAL(wc.flags, MY_DISK_CTL_FLAG | BLOB_DISK_CTL_CHUNKED_CSUM);
	BOOST_REQUIRE_EQUAL(wc.size, sizeof(data));
	BOOST_REQUIRE_EQUAL(wc.total_data_size, sizeof(data));

	BOOST_REQUIRE_EQUAL(eblob_verify_checksum(wrapper.get(), &key, &wc), 0);
	// read current byte
	char current_byte;
	BOOST_REQUIRE_EQUAL(__eblob_read_ll(wc.data_fd, &current_byte, sizeof(current_byte), wc.data_offset), 0);
	// corrupt data
	BOOST_REQUIRE_EQUAL(__eblob_write_ll(wc.data_fd, "a", 1, wc.data_offset), 0);

	BOOST_REQUIRE_EQUAL(eblob_verify_checksum(wrapper.get(), &key, &wc), -EILSEQ);
	// verify checksum via read
	BOOST_REQUIRE_EQUAL(eblob_read_return(wrapper.get(), &key, EBLOB_READ_CSUM, &wc), -EILSEQ);
	// read @wc without checksum verification
	BOOST_REQUIRE_EQUAL(eblob_read_return(wrapper.get(), &key, EBLOB_READ_NOCSUM, &wc), 0);
	// check that the record was marked corrupted
	BOOST_REQUIRE_EQUAL(wc.flags, MY_DISK_CTL_FLAG | BLOB_DISK_CTL_CHUNKED_CSUM | BLOB_DISK_CTL_CORRUPTED);
	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 1);

	// restore data
	BOOST_REQUIRE_EQUAL(__eblob_write_ll(wc.data_fd, &current_byte, sizeof(current_byte), wc.data_offset), 0);
	// checksum verification should succeed
	BOOST_REQUIRE_EQUAL(eblob_verify_checksum(wrapper.get(), &key, &wc), 0);
	// read @wc with checksum verification should succeed
	BOOST_REQUIRE_EQUAL(eblob_read_return(wrapper.get(), &key, EBLOB_READ_CSUM, &wc), 0);
	// the record should still be marked corrupted
	// TODO(shaitan): maybe we should drop BLOB_DISK_CTL_CORRUPTED if whole record verification has succeeded
	// TODO(shaitan): but we can't drop BLOB_DISK_CTL_CORRUPTED if only part of the record was successfully verified
	BOOST_REQUIRE_EQUAL(wc.flags, MY_DISK_CTL_FLAG | BLOB_DISK_CTL_CHUNKED_CSUM | BLOB_DISK_CTL_CORRUPTED);

	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 1);

	wrapper.restart();

	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 1);
	BOOST_REQUIRE_EQUAL(eblob_remove(wrapper.get(), &key), 0);
	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 0);
	wrapper.stop();
}

BOOST_AUTO_TEST_CASE(test_footer_corruption) {
	/* corrupt record's footer and check that the record is considered as corrupted */
	eblob_wrapper wrapper;
	BOOST_REQUIRE(wrapper.get() != nullptr);

	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 0);

	auto key = hash("some key");
	constexpr char data[] = "some data";
	constexpr uint64_t MY_DISK_CTL_FLAG = 1 << 30;

	eblob_write_control wc;
	BOOST_REQUIRE_EQUAL(
		eblob_write_return(wrapper.get(), &key, (void *)data, /*offset*/ 0, sizeof(data), MY_DISK_CTL_FLAG, &wc),
		0);
	BOOST_REQUIRE_EQUAL(wc.flags, MY_DISK_CTL_FLAG | BLOB_DISK_CTL_CHUNKED_CSUM);

	const uint64_t footer_offset = wc.data_offset + wc.total_data_size;

	// read current byte
	char current_byte;
	BOOST_REQUIRE_EQUAL(__eblob_read_ll(wc.data_fd, &current_byte, sizeof(current_byte), footer_offset), 0);
	// corrupt footer
	BOOST_REQUIRE_EQUAL(__eblob_write_ll(wc.data_fd, "a", 1, footer_offset), 0);

	BOOST_REQUIRE_EQUAL(eblob_verify_checksum(wrapper.get(), &key, &wc), -EILSEQ);
	// verify checksum via read
	BOOST_REQUIRE_EQUAL(eblob_read_return(wrapper.get(), &key, EBLOB_READ_CSUM, &wc), -EILSEQ);
	// read @wc without checksum verification
	BOOST_REQUIRE_EQUAL(eblob_read_return(wrapper.get(), &key, EBLOB_READ_NOCSUM, &wc), 0);
	// check that the record was marked corrupted
	BOOST_REQUIRE_EQUAL(wc.flags, MY_DISK_CTL_FLAG | BLOB_DISK_CTL_CHUNKED_CSUM | BLOB_DISK_CTL_CORRUPTED);
	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 1);

	// restore footer
	BOOST_REQUIRE_EQUAL(__eblob_write_ll(wc.data_fd, &current_byte, sizeof(current_byte), footer_offset), 0);
	// checksum verification should succeed
	BOOST_REQUIRE_EQUAL(eblob_verify_checksum(wrapper.get(), &key, &wc), 0);
	// read @wc with checksum verification should succeed
	BOOST_REQUIRE_EQUAL(eblob_read_return(wrapper.get(), &key, EBLOB_READ_CSUM, &wc), 0);
	// the record should still be marked corrupted
	// TODO(shaitan): maybe we should drop BLOB_DISK_CTL_CORRUPTED if whole 1 verification has succeeded
	// TODO(shaitan): but we can't drop BLOB_DISK_CTL_CORRUPTED if only part of the record was successfully verified
	BOOST_REQUIRE_EQUAL(wc.flags, MY_DISK_CTL_FLAG | BLOB_DISK_CTL_CHUNKED_CSUM | BLOB_DISK_CTL_CORRUPTED);

	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 1);

	wrapper.restart();

	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 1);
	BOOST_REQUIRE_EQUAL(eblob_remove(wrapper.get(), &key), 0);
	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 0);
}
