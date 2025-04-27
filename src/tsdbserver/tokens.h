// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __TSDBSERVER_TOKENS_H
#define __TSDBSERVER_TOKENS_H

#include <hdr/delegate.h>
#include <stdint.h>
#include <vector>

enum command_token : uint32_t
{
    CT_CREATE_DATABASE      = 0x60545A42,
    CT_CREATE_MEASUREMENT   = 0xBB632CE1,
    CT_WRITE_POINTS         = 0xEAF5E003,
    CT_SELECT_POINTS_LIMIT  = 0x7446C560,
    CT_SELECT_POINTS_LAST   = 0x76CF2220,
    CT_DELETE_POINTS        = 0xD9082F2C,
    CT_GET_SCHEMA           = 0x87E5A959,
    CT_LIST_DATABASES       = 0x29200D6D,
    CT_LIST_MEASUREMENTS    = 0x0FEB1399,
    CT_LIST_SERIES          = 0x7B8238D6,
    CT_ACTIVE_SERIES        = 0xF3B5093D,
    CT_COUNT_POINTS         = 0x0E329B19,
    CT_SUM_POINTS           = 0x90305A39,
    CT_NOP                  = 0x22CF1296,
    CT_AUTHENTICATE         = 0x0995EBDA,
};

enum data_token : uint32_t
{
    DT_DATABASE         = 0x39385A4F,   // <database>
    DT_MEASUREMENT      = 0xDC1F48F3,   // <measurement>
    DT_SERIES           = 0x4E873749,   // <series>
    DT_TYPED_FIELDS     = 0x02AC7330,   // <f1>/<type1>,<f2>/<type2>,...
    DT_FIELD_LIST       = 0xBB62ACC3,   // <f1>,<f2>,...
    DT_CHUNK            = 0xE4E8518F,   // <chunk header>, then data
    DT_TIME_FIRST       = 0x55BA37B4,   // <t0> (uint64_t)
    DT_TIME_LAST        = 0xC4EE45BA,   // <t1> (uint64_t)
    DT_NLIMIT           = 0xEEF2BB02,   // LIMIT <N> (uint64_t)
    DT_NLAST            = 0xD74F10A3,   // LAST <N> (uint64_t)
    DT_END              = 0x4E29ADCC,   // end of command
    DT_STATUS_CODE      = 0x8C8C07D9,   // <errno> (uint32_t)
    DT_FIELD_TYPE       = 0x7DB40C2A,   // <type> (uint32_t)
    DT_FIELD_NAME       = 0x5C0D45C1,   // <name>
    DT_READY_FOR_CHUNK  = 0x6000531C,   // <max_data_len> (uint32_t)
    DT_NPOINTS          = 0x5F469D08,   // <npoints> (uint64_t)
    DT_WINDOW_NS        = 0x76F0C374,   // <window_ns> (uint64_t)
    DT_SUMS_CHUNK       = 0x53FC76FC,   // <chunk_npoints> (uint16_t)
    DT_USERNAME         = 0x6E39D1DE,   // <username>
    DT_PASSWORD         = 0x602E5B01,   // <password>
};

struct chunk_header
{
    uint32_t    npoints;
    uint32_t    bitmap_offset;
    uint32_t    data_len;
    uint8_t     data[];
};

struct parsed_data_token
{
    data_token type;
    const char* data;
    union
    {
        size_t      len;
        uint64_t    u64;
    };

    std::string to_string() const {return std::string(data,len);}

    parsed_data_token():
        data(NULL)
    {
    }

    parsed_data_token(const parsed_data_token&) = delete;

    parsed_data_token(parsed_data_token&& other):
        type(other.type),
        data(other.data),
        u64(other.u64)
    {
        other.data = NULL;
    }

    ~parsed_data_token()
    {
        free((void*)data);
        data = NULL;
    }
};

template<typename ...Args>
struct command_syntax
{
    delegate<void(Args..., const std::vector<parsed_data_token>&)>  handler;
    const command_token cmd_token;
    const std::vector<data_token> data_tokens;
};

#endif /* __TSDBSERVER_TOKENS_H */
