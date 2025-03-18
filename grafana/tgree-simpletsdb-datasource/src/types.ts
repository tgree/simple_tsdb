import { DataSourceJsonData } from '@grafana/data';
import { DataQuery } from '@grafana/schema';

export interface MyQuery extends DataQuery {
  measurement?: string;
  series?: string;
  field?: string;
}

export const DEFAULT_QUERY: Partial<MyQuery> = {
};

export interface DataPoint {
  Time: number;
  Value: number;
}

export interface DataSourceResponse {
  datapoints: DataPoint[];
}

/**
 * These are options configured for each DataSource instance
 */
export interface MyDataSourceOptions extends DataSourceJsonData {
  database?: string;
}

/**
 * Value that is used in the backend, but never sent over HTTP to the frontend
 */
export interface MySecureJsonData {
}

export type DatabasesResponse = {
    databases: string[];
};

export type MeasurementsResponse = {
    measurements: string[];
};

export type SeriesResponse = {
    series: string[];
};
