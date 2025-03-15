import React, { ChangeEvent } from 'react';
import { InlineField, Input, Stack } from '@grafana/ui';
import { QueryEditorProps } from '@grafana/data';
import { DataSource } from '../datasource';
import { MyDataSourceOptions, MyQuery } from '../types';

type Props = QueryEditorProps<DataSource, MyQuery, MyDataSourceOptions>;

export function QueryEditor({ query, onChange, onRunQuery }: Props) {
  const onMeasurementChange = (event: ChangeEvent<HTMLInputElement>) => {
    onChange({ ...query, measurement: event.target.value });
  };
  const onSeriesChange = (event: ChangeEvent<HTMLInputElement>) => {
    onChange({ ...query, series: event.target.value });
  };
  const onFieldChange = (event: ChangeEvent<HTMLInputElement>) => {
    onChange({ ...query, field: event.target.value });
  };

  const { measurement, series, field } = query;

  return (
    <div>
      <InlineField label="Measurement" labelWidth={16} tooltip="Not used yet">
        <Input
          id="query-editor-measurement"
          onChange={onMeasurementChange}
          value={measurement || ''}
          required
          placeholder="Enter a measurement"
        />
      </InlineField>
      <InlineField label="Series" labelWidth={16} tooltip="Not used yet">
        <Input
          id="query-editor-series"
          onChange={onSeriesChange}
          value={series || ''}
          required
          placeholder="Enter a series"
        />
      </InlineField>
      <InlineField label="Field" labelWidth={16} tooltip="Not used yet">
        <Input
          id="query-editor-field"
          onChange={onFieldChange}
          value={field || ''}
          required
          placeholder="Enter a field"
        />
      </InlineField>
    </div>
  );
}
