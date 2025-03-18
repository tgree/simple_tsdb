import { DataSourceWithBackend, getTemplateSrv } from '@grafana/runtime';
import type { DataSourceInstanceSettings, ScopedVars } from '@grafana/data';
import type { BasicQuery, BasicDataSourceOptions, DatabasesResponse, MeasurementsResponse,
              SeriesResponse } from './types';

export class BasicDataSource extends DataSourceWithBackend<BasicQuery, BasicDataSourceOptions> {
  database: string;

  constructor(instanceSettings: DataSourceInstanceSettings<BasicDataSourceOptions>) {
    super(instanceSettings);

    this.database = instanceSettings.jsonData.database || "";
  }

  applyTemplateVariables(query: BasicQuery, scopedVars: ScopedVars) {
    return {
      ...query,
      measurement: getTemplateSrv().replace(query.measurement, scopedVars),
      series: getTemplateSrv().replace(query.series, scopedVars),
      field: getTemplateSrv().replace(query.field, scopedVars),
    };
  }

  filterQuery(query: BasicQuery): boolean {
    return true;
  }

  getDatabases(): Promise<DatabasesResponse> {
    return this.getResource('/databases');
  }

  getMeasurements(database: string): Promise<MeasurementsResponse> {
    return this.getResource('/measurements?database=' + database);
  }

  getSeries(database: string, measurement: string): Promise<SeriesResponse> {
    return this.getResource('/measurements?database=' + database + '&measurement=' + measurement);
  }
}
