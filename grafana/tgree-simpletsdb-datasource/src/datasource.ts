import { DataSourceInstanceSettings, CoreApp, ScopedVars } from '@grafana/data';
import { DataSourceWithBackend, getTemplateSrv } from '@grafana/runtime';

import { MyQuery, MyDataSourceOptions, DEFAULT_QUERY, DatabasesResponse, MeasurementsResponse,
         SeriesResponse } from './types';

export class DataSource extends DataSourceWithBackend<MyQuery, MyDataSourceOptions> {
  constructor(instanceSettings: DataSourceInstanceSettings<MyDataSourceOptions>) {
    super(instanceSettings);
  }

  getDefaultQuery(_: CoreApp): Partial<MyQuery> {
    return DEFAULT_QUERY;
  }

  applyTemplateVariables(query: MyQuery, scopedVars: ScopedVars) {
    return {
      ...query,
      measurement: getTemplateSrv().replace(query.measurement, scopedVars),
      series: getTemplateSrv().replace(query.series, scopedVars),
      field: getTemplateSrv().replace(query.field, scopedVars),
    };
  }

  filterQuery(query: MyQuery): boolean {
    // if no query has been provided, prevent the query from being executed
    return !!query.measurement && !!query.series && !!query.field;
  }

  getDatabases(): Promise<DatabasesResponse> {
    return this.getResource("/databases");
  }

  getMeasurements(database: String): Promise<MeasurementsResponse> {
    return this.getResource("/measurements?database=" + database);
  }

  getSeries(database: String, measurement: String): Promise<SeriesResponse> {
    return this.getResource("/series?database=" + database + "&measurement=" + measurement);
  }
}
