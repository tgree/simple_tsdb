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
    
    struct no_such_database_exception : public exception
    {
        // The specified database does not exist.
        virtual const char* what() const noexcept override
        {
            return "No such database.";
        }

        no_such_database_exception():exception(NO_SUCH_DATABASE) {}
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

    struct no_such_measurement_exception : public exception
    {
        // The specified measurement does not exist.
        virtual const char* what() const noexcept override
        {
            return "No such measurement.";
        }

        no_such_measurement_exception():exception(NO_SUCH_MEASUREMENT) {}
    };

    struct invalid_measurement_exception : public exception
    {
        // The specified path is not in <database>/<measurement> form.
        virtual const char* what() const noexcept override
        {
            return "Invalid measurement path.";
        }

        invalid_measurement_exception():exception(INVALID_MEASUREMENT) {}
    };

    struct measurement_exists_exception : public exception
    {
        // The specified measurement already exists and has a different schema
        // from the one we tried to create.
        virtual const char* what() const noexcept override
        {
            return "Measurement already exists.";
        }

        measurement_exists_exception():exception(MEASUREMENT_EXISTS) {}
    };

    struct invalid_series_exception : public exception
    {
        // The specified path is not in <database>/<measurement>/<series> form.
        virtual const char* what() const noexcept override
        {
            return "Invalid series path.";
        }

        invalid_series_exception():exception(INVALID_SERIES) {}
    };

    struct no_such_series_exception : public exception
    {
        // The specified series does not exist.
        virtual const char* what() const noexcept override
        {
            return "No such series.";
        }

        no_such_series_exception():exception(NO_SUCH_SERIES) {}
    };

    struct corrupt_schema_file_exception : public exception
    {
        // The schema.txt file being parsed was malformed.
        virtual const char* what() const noexcept override
        {
            return "Invalid schema file.";
        }

        corrupt_schema_file_exception():exception(CORRUPT_SCHEMA_FILE) {}
    };

    struct no_such_field_exception : public exception
    {
        // The specified field was not part of a measurement's schema.
        virtual const char* what() const noexcept override
        {
            return "No such field.";
        }

        no_such_field_exception():exception(NO_SUCH_FIELD) {}
    };

    struct end_of_select_exception : public exception
    {
        // Tried to advance() past the end of a select_op result.
        virtual const char* what() const noexcept override
        {
            return "End of select_op.";
        }

        end_of_select_exception():exception(END_OF_SELECT) {}
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

    struct out_of_order_timestamps_exception : public exception
    {
        // The timestamps passed to write_series() were not in strictly-
        // increasing order.
        virtual const char* what() const noexcept override
        {
            return "Out of order timestamps.";
        }

        out_of_order_timestamps_exception():exception(OUT_OF_ORDER_TIMESTAMPS) {}
    };

    struct timestamp_overwrite_mismatch_exception : public exception
    {
        // When overwriting the tail of a series, the new timestamps didn't
        // match the old timestamps.
        virtual const char* what() const noexcept override
        {
            return "Timestamp overwrite mismatch.";
        }

        timestamp_overwrite_mismatch_exception():
            exception(TIMESTAMP_OVERWRITE_MISMATCH)
        {
        }
    };

    struct field_overwrite_mismatch_exception : public exception
    {
        // When overwriting the tail of a series, the new field contents didn't
        // match the old field contents for a given timestamp.
        virtual const char* what() const noexcept override
        {
            return "Field overwrite mistmatch.";
        }

        field_overwrite_mismatch_exception():
            exception(FIELD_OVERWRITE_MISMATCH)
        {
        }
    };

    struct bitmap_overwrite_mismatch_exception : public exception
    {
        // When overwriting the tail of a series, the new bitmap contents didn't
        // match the old bitmap contents for a given timestamp.
        virtual const char* what() const noexcept override
        {
            return "Bitmap overwrite mistmatch.";
        }

        bitmap_overwrite_mismatch_exception():
            exception(BITMAP_OVERWRITE_MISMATCH)
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

    struct user_exists_exception : public exception
    {
        // tsdb::add_user() was called for a user that already exists.
        virtual const char* what() const noexcept override
        {
            return "User already exists.";
        }

        user_exists_exception():
            exception(USER_EXISTS)
        {
        }
    };

    struct no_such_user_exception : public exception
    {
        // tsdb::verify_user() was called for a user that doesn't exist.
        virtual const char* what() const noexcept override
        {
            return "No such user.";
        }

        no_such_user_exception():
            exception(NO_SUCH_USER)
        {
        }
    };

    struct not_a_tsdb_root : public exception
    {
        // Failed to open the expected files at the specified root location.
        virtual const char* what() const noexcept override
        {
            return "Not a TSDB root directory.";
        }

        not_a_tsdb_root():
            exception(NOT_A_TSDB_ROOT)
        {
        }
    };
}

#endif /* __SRC_LIBTSDB_EXCEPTION_H */
