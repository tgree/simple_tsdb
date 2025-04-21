import type { DataSourceJsonData } from '@grafana/data';
import type { DataQuery } from '@grafana/schema';

export interface BasicQuery extends DataQuery {
  measurement?: string;
  series?: string;
  field?: string;
  transform?: string;
  zoom?: string;
  alias?: string;
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

export type DatabaseList = {
  databases: string[];
};

export type MeasurementList = {
  measurements: string[];
};

export type SeriesList = {
  series: string[];
};

export type FieldsList = {
  fields: string[];
};
