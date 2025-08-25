// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "measurement.h"
#include "database.h"
#include "count.h"
#include "exception.h"
#include <futil/xact.h>
#include <hdr/types.h>

// TODO: Annoyingly, this doesn't properly detect a measurement directory that
//       exists but has not schema file.  That will throw
//       no_such_measurement_exception but put create_measurement() into an
//       infinite retry loop.
//
//       For now, we have adjusted create_measurement() to only retry once;
//       that should be sufficient to resolve any race condition while still
//       providing an exit case where we can't actually open the measurement
//       because it is somehow corrupt.
tsdb::measurement::measurement(const database& db, const futil::path& path) try:
    db(db),
    dir(db.dir,path),
    schema_fd(dir,"schema",O_RDONLY),
    schema_mapping(0,schema_fd.lseek(0,SEEK_END),PROT_READ,MAP_SHARED,
                   schema_fd.fd,0),
    fields((const schema_entry*)schema_mapping.addr,
           (const schema_entry*)schema_mapping.addr +
           schema_mapping.len / sizeof(schema_entry))
{
    size_t offset = 0;
    for (size_t i=0; i<fields.size(); ++i)
    {
        const auto& f = fields[i];
        if (f.type == 0 || f.type > LAST_FIELD_TYPE || f.name[0] == '\0' ||
            f.name[123] != '\0' || f.version != SCHEMA_VERSION ||
            f.index != i || f.offset != offset)
        {
            throw tsdb::corrupt_schema_file_exception();
        }
        offset += ftinfos[f.type].nbytes;
    }
}
catch (const futil::errno_exception& e)
{
    if (e.errnov == ENOENT)
        throw tsdb::no_such_measurement_exception();
    throw;
}

std::vector<std::string>
tsdb::measurement::list_active_series(uint64_t t0, uint64_t t1) const
{
    std::vector<std::string> active_series;

    for (auto& series_name : list_series())
    {
        series_read_lock srl(*this,series_name);
        auto cr = tsdb::count_points(srl,t0,t1);
        if (cr.npoints)
            active_series.emplace_back(std::move(series_name));
    }

    return active_series;
}

void
tsdb::create_measurement(const database& db, const futil::path& name,
    const std::vector<schema_entry>& fields) try
{
    if (name.empty() || name[0] == '/' || name.count_components() > 1)
        throw tsdb::invalid_measurement_exception();
    if (fields.size() > MAX_FIELDS)
        throw tsdb::too_many_fields_exception();

    // Validate the schema.
    size_t offset = 0;
    for (size_t i=0; i<fields.size(); ++i)
    {
        const schema_entry& se = fields[i];
        kassert(se.version == SCHEMA_VERSION);
        kassert(se.index == i);
        kassert(se.offset == offset);
        kassert(se.name[NELEMS(se.name)-1] == '\0');
        kassert(se.type && se.type <= LAST_FIELD_TYPE);
        offset += ftinfos[se.type].nbytes;
        for (size_t j=i+1; j<fields.size(); ++j)
        {
            if (!strcmp(se.name,fields[j].name))
                throw tsdb::duplicate_field_exception();
        }
    }

    for (size_t i=0; i<2; ++i)
    {
        try
        {
            // Start by trying to open an existing measurement.
            tsdb::measurement m(db,name);

            // Validate that it is the same as what we are creating.
            if (fields.size() != m.fields.size())
                throw tsdb::measurement_exists_exception();
            for (size_t i=0; i<fields.size(); ++i)
            {
                if (fields[i].type != m.fields[i].type)
                    throw tsdb::measurement_exists_exception();
                if (strcmp(fields[i].name,m.fields[i].name))
                    throw tsdb::measurement_exists_exception();
            }

            // The measurement exists and is the same, so just exit.
            return;
        }
        catch (const tsdb::no_such_measurement_exception&)
        {
        }

        // The measurement doesn't exist.  Create it in the tmp directory first.
        futil::xact_mkdtemp m_dir(db.root.tmp_dir,0770);
        futil::xact_creat schema_fd(m_dir,"schema",O_WRONLY | O_CREAT | O_EXCL,
                                    0440);

        // Write the schema and fsync().
        schema_fd.write_all(&fields[0],fields.size()*sizeof(schema_entry));
        m_dir.fsync();
        schema_fd.fsync_and_barrier();

        // Try to move the newly-created measurement into place.
        if (futil::rename_if_not_exists(db.root.tmp_dir,(const char*)m_dir.name,
                                        db.dir,name))
        {
            m_dir.fsync_and_flush();
            db.dir.fsync_and_flush();
            db.root.tmp_dir.fsync_and_flush();
            schema_fd.commit();
            m_dir.commit();
            return;
        }

        // The move operation failed.  This means that a directory now exists
        // in the target measurement location, indicating that someone has just
        // created the measurement.  Loop around and try again.
    }

    // The measurement exists, but we can't open it for some reason.  It must
    // be corrupt.
    throw tsdb::corrupt_measurement_exception();
}
catch (const futil::errno_exception& e)
{
    throw tsdb::create_measurement_io_error_exception(e.errnov);
}
