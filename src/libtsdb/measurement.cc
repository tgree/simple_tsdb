// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "measurement.h"
#include "database.h"
#include "exception.h"
#include <futil/xact.h>

tsdb::measurement::measurement(const database& db, const futil::path& path) try:
    dir(db.dir,path),
    schema_fd(dir,"schema",O_RDONLY),
    schema_mapping(0,schema_fd.lseek(0,SEEK_END),PROT_READ,MAP_SHARED,
                   schema_fd.fd,0),
    fields((const schema_entry*)schema_mapping.addr,
           (const schema_entry*)schema_mapping.addr +
           schema_mapping.len / sizeof(schema_entry))
{
    for (const auto& f : fields)
    {
        if (f.type == 0 || f.type > LAST_FIELD_TYPE || f.name[0] == '\0' ||
            f.name[123] != '\0')
        {
            throw tsdb::corrupt_schema_file_exception();
        }
    }
}
catch (const futil::errno_exception& e)
{
    if (e.errnov == ENOENT)
        throw tsdb::no_such_measurement_exception();
    throw;
}

void
tsdb::create_measurement(const database& db, const futil::path& name,
    const std::vector<schema_entry>& fields) try
{
    if (name.empty() || name[0] == '/' || name.count_components() > 1)
        throw tsdb::invalid_measurement_exception();

    for (;;)
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
        futil::directory tmp_dir("tmp");
        futil::xact_mkdtemp m_dir(tmp_dir,"measurement.XXXXXX",0770);
        futil::xact_creat csl_fd(m_dir,"create_series_lock",
                                 O_WRONLY | O_CREAT | O_EXCL,0660);
        futil::xact_creat schema_fd(m_dir,"schema",O_WRONLY | O_CREAT | O_EXCL,
                                    0440);
        
        for (auto& se : fields)
            schema_fd.write_all(&se,sizeof(se));

        // Try to move the newly-created measurement into place.
        if (futil::rename_if_not_exists(tmp_dir,(const char*)m_dir.name,db.dir,
                                        name))
        {
            schema_fd.commit();
            csl_fd.commit();
            m_dir.commit();
            return;
        }

        // The move operation failed.  This means that a directory now exists
        // in the target measurement location, indicating that someone has just
        // created the measurement.  Loop around and try again.
    }
}
catch (const futil::errno_exception& e)
{
    if (e.errnov == ENOENT)
        throw tsdb::no_such_database_exception();
    throw tsdb::create_measurement_io_error_exception(e.errnov);
}
