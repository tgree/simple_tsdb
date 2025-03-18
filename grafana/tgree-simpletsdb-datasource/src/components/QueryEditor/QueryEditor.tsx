import React, { ChangeEvent } from 'react';
import { InlineField, Input } from '@grafana/ui';
import { QueryEditorProps } from '@grafana/data';
import { DataSource } from '../datasource';
import { MyDataSourceOptions, MyQuery } from '../types';

type Props = QueryEditorProps<DataSource, MyQuery, MyDataSourceOptions>;

export function QueryEditor(props: Props) {

  const onMeasurementChange = (event: ChangeEvent<HTMLInputElement>) => {
    props.onChange({ ...props.query, measurement: event.target.value });
  };

  const onSeriesChange = (event: ChangeEvent<HTMLInputElement>) => {
    props.onChange({ ...props.query, series: event.target.value });
  };

  const onFieldChange = (event: ChangeEvent<HTMLInputElement>) => {
    props.onChange({ ...props.query, field: event.target.value });
  };

  const { measurement, series, field } = props.query;

  return (
    <div>
      <InlineField label="Measurement" labelWidth={16} tooltip="Not used yet">
        <Input
          id="query-editor-measurement"
          onChange={onMeasurementChange}
          value={measurement || props.datasource.getMeasurements(props.datasource.database).measurements[0] || ''}
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
