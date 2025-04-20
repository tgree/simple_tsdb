import { DataSourceWithBackend, getTemplateSrv } from '@grafana/runtime';
import type { DataSourceInstanceSettings, ScopedVars } from '@grafana/data';
import type { BasicQuery, BasicDataSourceOptions, DatabaseList, MeasurementList,
              SeriesList, FieldsList } from './types';

export class BasicDataSource extends DataSourceWithBackend<BasicQuery, BasicDataSourceOptions> {
  database: string;

  constructor(instanceSettings: DataSourceInstanceSettings<BasicDataSourceOptions>) {
    super(instanceSettings);

    this.database = instanceSettings.jsonData.database || "";
  }

  applyTemplateVariables(query: BasicQuery, scopedVars: ScopedVars) {
    return {
      ...query,
      measurement: getTemplateSrv().replace(query.measurement, scopedVars, 'text'),
      series: getTemplateSrv().replace(query.series, scopedVars, 'text'),
      field: getTemplateSrv().replace(query.field, scopedVars, 'text'),
    };
  }

  async metricFindQuery(query: string, options?: any) {
      const sl = await this.getSeriesList(this.database, query);
      const values = sl.series.map(s => ({text: s}));
      return values
  }

  filterQuery(query: BasicQuery): boolean {
    return true;
  }

  getDatabaseList(): Promise<DatabaseList> {
    return this.getResource('/databases');
  }

  getMeasurementList(database: string): Promise<MeasurementList> {
    return this.getResource('/measurements?database=' + database);
  }

  getSeriesList(database: string, measurement: string): Promise<SeriesList> {
    return this.getResource('/series?database=' + database + '&measurement=' + measurement);
  }

  getFieldsList(database: string, measurement: string): Promise<FieldsList> {
    return this.getResource('/fields?database=' + database + '&measurement=' + measurement);
  }
}
