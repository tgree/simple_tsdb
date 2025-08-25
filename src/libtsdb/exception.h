// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_LIBTSDB_EXCEPTION_H
#define __SRC_LIBTSDB_EXCEPTION_H

#include <exception>
#include <string.h>
#include <sys/types.h>
#include <stdint.h>

namespace tsdb
{
    enum status_code
    {
        INIT_IO_ERROR                   = -1,
        CREATE_DATABASE_IO_ERROR        = -2,
        CREATE_MEASUREMENT_IO_ERROR     = -3,
        INVALID_MEASUREMENT             = -4,
        INVALID_SERIES                  = -5,
        CORRUPT_SCHEMA_FILE             = -6,
        NO_SUCH_FIELD                   = -7,
        END_OF_SELECT                   = -8,
        INCORRECT_WRITE_CHUNK_LEN       = -9,
        OUT_OF_ORDER_TIMESTAMPS         = -10,
        TIMESTAMP_OVERWRITE_MISMATCH    = -11,
        FIELD_OVERWRITE_MISMATCH        = -12,
        BITMAP_OVERWRITE_MISMATCH       = -13,
        TAIL_FILE_TOO_BIG               = -14,
        TAIL_FILE_INVALID_SIZE          = -15,
        INVALID_TIME_LAST               = -16,
        NO_SUCH_SERIES                  = -17,
        NO_SUCH_DATABASE                = -18,
        NO_SUCH_MEASUREMENT             = -19,
        MEASUREMENT_EXISTS              = -20,
        USER_EXISTS                     = -21,
        NO_SUCH_USER                    = -22,
        NOT_A_TSDB_ROOT                 = -23,
        DUPLICATE_FIELD                 = -24,
        TOO_MANY_FIELDS                 = -25,
        INVALID_CONFIG_FILE             = -26,
        INVALID_CHUNK_SIZE              = -27,
        CORRUPT_MEASUREMENT             = -28,
    };

    struct exception : public std::exception
    {
        const status_code sc;

        exception(status_code sc):sc(sc) {}
    };

    struct errno_exception : public exception
    {
        const int errnov;

        virtual const char* what() const noexcept override
        {
            return strerror(errnov);
        }

        errno_exception(status_code sc, int errnov):
            exception(sc),
            errnov(errnov)
        {
        }
    };

    struct init_io_error_exception : public errno_exception
    {
        init_io_error_exception(int errnov):
            errno_exception(INIT_IO_ERROR,errnov)
        {
        }
    };

    struct create_database_io_error_exception : public errno_exception
    {
        virtual const char* what() const noexcept override
        {
            return "I/O error creating database.";
        }

        create_database_io_error_exception(int errnov):
            errno_exception(CREATE_DATABASE_IO_ERROR,errnov)
        {
        }
    };

    struct create_measurement_io_error_exception : public errno_exception
    {
        virtual const char* what() const noexcept override
        {
            return "I/O error creating measurement.";
        }

        create_measurement_io_error_exception(int errnov):
            errno_exception(CREATE_MEASUREMENT_IO_ERROR,errnov)
        {
        }
    };

    struct incorrect_write_chunk_len_exception : public exception
    {
        const size_t expected_len;
        const size_t chunk_len;

        // The length of the data buffer passed in to write_series() was
        // incorrect for the specified number of points and the measurement's
        // schema.
        virtual const char* what() const noexcept override
        {
            return "Incorrect chunk length.";
        }

        incorrect_write_chunk_len_exception(size_t expected_len,
                                            size_t chunk_len):
            exception(INCORRECT_WRITE_CHUNK_LEN),
            expected_len(expected_len),
            chunk_len(chunk_len)
        {
        }
    };

    struct corrupt_series_exception : public exception
    {
        corrupt_series_exception(status_code sc):exception(sc) {}
    };

    struct tail_file_too_big_exception : public corrupt_series_exception
    {
        const off_t size;

        // A tail file in the timestamp index is larger than 2M.
        virtual const char* what() const noexcept override
        {
            return "Tail file too large.";
        }

        tail_file_too_big_exception(const off_t size):
            corrupt_series_exception(TAIL_FILE_TOO_BIG),
            size(size)
        {
        }
    };

    struct tail_file_invalid_size_exception : public corrupt_series_exception
    {
        const off_t size;

        // A tail file in the timestamp index is not a multiple of 64-bits.
        virtual const char* what() const noexcept override
        {
            return "Tail file has invalid length.";
        }

        tail_file_invalid_size_exception(const off_t size):
            corrupt_series_exception(TAIL_FILE_INVALID_SIZE),
            size(size)
        {
        }
    };

    struct invalid_time_last_exception : public corrupt_series_exception
    {
        const uint64_t tail_time_ns;
        const uint64_t time_last_ns;

        // The time_last file should always have a timestamp that matches a
        // timestamp in a timestamp file.  Specifically, the last valid entry in
        // the timestamp tail file should be the same value as time_last.
        virtual const char* what() const noexcept override
        {
            return "Tail file last timestamp not same as time_last timestamp.";
        }

        invalid_time_last_exception(uint64_t tail_time_ns,
                                    uint64_t time_last_ns):
            corrupt_series_exception(INVALID_TIME_LAST),
            tail_time_ns(tail_time_ns),
            time_last_ns(time_last_ns)
        {
        }
    };
    
#define DEFINE_TSDB_EXCEPTION(ename,eval,estr)\
    struct ename : public exception \
    { \
        virtual const char* what() const noexcept override \
        { \
            return estr; \
        } \
        ename():exception(eval) {} \
    }

DEFINE_TSDB_EXCEPTION(no_such_database_exception,NO_SUCH_DATABASE,
                      "No such database.");
DEFINE_TSDB_EXCEPTION(no_such_measurement_exception,NO_SUCH_MEASUREMENT,
                      "No such measurement.");
DEFINE_TSDB_EXCEPTION(invalid_measurement_exception,INVALID_MEASUREMENT,
                      "Invalid measurement path.");
DEFINE_TSDB_EXCEPTION(measurement_exists_exception,MEASUREMENT_EXISTS,
                      "Measurement already exists.");
DEFINE_TSDB_EXCEPTION(invalid_series_exception,INVALID_SERIES,
                      "Invalid series path.");
DEFINE_TSDB_EXCEPTION(no_such_series_exception,NO_SUCH_SERIES,
                      "No such series.");
DEFINE_TSDB_EXCEPTION(corrupt_schema_file_exception,CORRUPT_SCHEMA_FILE,
                      "Invalid schema file.");
DEFINE_TSDB_EXCEPTION(no_such_field_exception,NO_SUCH_FIELD,"No such field.");
DEFINE_TSDB_EXCEPTION(end_of_select_exception,END_OF_SELECT,
                      "End of select_op.");
DEFINE_TSDB_EXCEPTION(out_of_order_timestamps_exception,OUT_OF_ORDER_TIMESTAMPS,
                      "Out of order timestamps.");
DEFINE_TSDB_EXCEPTION(timestamp_overwrite_mismatch_exception,
                      TIMESTAMP_OVERWRITE_MISMATCH,
                      "Timestamp overwrite mismatch.");
DEFINE_TSDB_EXCEPTION(field_overwrite_mismatch_exception,
                      FIELD_OVERWRITE_MISMATCH,"Field overwrite mistmatch.");
DEFINE_TSDB_EXCEPTION(bitmap_overwrite_mismatch_exception,
                      BITMAP_OVERWRITE_MISMATCH,"Bitmap overwrite mistmatch.");
DEFINE_TSDB_EXCEPTION(user_exists_exception,USER_EXISTS,"User already exists.");
DEFINE_TSDB_EXCEPTION(no_such_user_exception,NO_SUCH_USER,"No such user.");
DEFINE_TSDB_EXCEPTION(not_a_tsdb_root,NOT_A_TSDB_ROOT,
                      "Not a TSDB root directory.");
DEFINE_TSDB_EXCEPTION(duplicate_field_exception,DUPLICATE_FIELD,
                      "Duplicate field requested.");
DEFINE_TSDB_EXCEPTION(too_many_fields_exception,TOO_MANY_FIELDS,
                      "Too many fields.");
DEFINE_TSDB_EXCEPTION(invalid_config_file_exception,INVALID_CONFIG_FILE,
                      "Invalid configuration file.");
DEFINE_TSDB_EXCEPTION(invalid_chunk_size_exception,INVALID_CHUNK_SIZE,
                      "Invalid chunk size.");
DEFINE_TSDB_EXCEPTION(corrupt_measurement_exception,CORRUPT_MEASUREMENT,
                      "Corrupt measurement.");
}

#endif /* __SRC_LIBTSDB_EXCEPTION_H */
