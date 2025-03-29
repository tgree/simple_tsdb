// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "measurement.h"
#include "database.h"
#include "exception.h"
#include <futil/xact.h>

tsdb::measurement::measurement(const database& db, const futil::path& path) try:
    dir(db.dir,path),
    schema_fd(dir,"schema",O_RDONLY | O_SHLOCK),
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

    // TODO: If we crash we will have a partially-created measurement which we
    // are then stuck with.  This should all be done in a temporary directory
    // that then gets atomically renamed into place.

    // TODO: If the measurement already exists and matches exactly what we are
    // trying to make, then we should quietly return success.

    futil::xact_mkdir m_dir(db.dir,name,0770);
    futil::xact_creat csl_fd(m_dir,"create_series_lock",O_WRONLY | O_CREAT,
                             0660);
    futil::xact_creat schema_fd(m_dir,"schema",
                                O_WRONLY | O_CREAT | O_EXCL | O_EXLOCK,0440);
    
    for (auto& se : fields)
        schema_fd.write_all(&se,sizeof(se));

    schema_fd.commit();
    csl_fd.commit();
    m_dir.commit();
}
catch (const futil::errno_exception& e)
{
    if (e.errnov == ENOENT)
        throw tsdb::no_such_database_exception();
    throw tsdb::create_measurement_io_error_exception(e.errnov);
}
