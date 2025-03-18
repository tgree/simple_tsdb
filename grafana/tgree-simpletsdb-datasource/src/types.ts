import type { DataSourceJsonData } from '@grafana/data';
import type { DataQuery } from '@grafana/schema';

export interface BasicQuery extends DataQuery {
  measurement?: string;
  series?: string;
  field?: string;
}

/**
 * These are options configured for each DataSource instance
 */
export interface BasicDataSourceOptions extends DataSourceJsonData {
  database?: string;
}

/**
 * Value that is used in the backend, but never sent over HTTP to the frontend
 */
export interface BasicSecureJsonData {
}

export type DatabasesResponse = {
  databases: string[];
};

export type MeasurementsResponse = {
  measurements: string[];
};
