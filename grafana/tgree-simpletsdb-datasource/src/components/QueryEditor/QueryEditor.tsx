import React, { ReactElement } from 'react';
import { useAsync } from 'react-use';
import { InlineField, Select } from '@grafana/ui';
import type { SelectableValue } from '@grafana/data';
import type { EditorProps } from './types';
import type { BasicDataSource } from '../../datasource';
import type { BasicQuery } from '../../types';

type AsyncMeasurementsState = {
  loading: boolean;
  measurements: Array<SelectableValue<string>>;
  error: Error | undefined;
};

function useMeasurements(datasource: BasicDataSource): AsyncMeasurementsState {
  const result = useAsync(async () => {
    const { measurements } = await datasource.getMeasurementList(datasource.database);

    return measurements.map((measurement) => ({
      label: measurement,
      value: measurement,
    }));
  }, [datasource]);

  return {
    loading: result.loading,
    measurements: result.value ?? [],
    error: result.error,
  };
}

function OnChangeMeasurement(selectable: SelectableValue<string>, props: EditorProps) {
  if (!selectable?.value) {
    return;
  }

  props.onChange({
    ...props.query,
    measurement: selectable.value,
  });
}

type AsyncSeriesState = {
  loading: boolean;
  series: Array<SelectableValue<string>>;
  error: Error | undefined;
};

function useSeries(datasource: BasicDataSource, query: BasicQuery): AsyncSeriesState {
  const result = useAsync(async () => {
    if (query.measurement == null) {
      return [];
    }

    const { series } = await datasource.getSeriesList(datasource.database, query.measurement!);

    return series.map((s) => ({
      label: s,
      value: s,
    }));
  }, [datasource, query]);

  return {
    loading: result.loading,
    series: result.value ?? [],
    error: result.error,
  };
}

function OnChangeSeries(selectable: SelectableValue<string>, props: EditorProps) {
  if (!selectable?.value) {
    return;
  }

  props.onChange({
    ...props.query,
    series: selectable.value,
  });
}

type AsyncFieldsState = {
  loading: boolean;
  fields: Array<SelectableValue<string>>;
  error: Error | undefined;
};

function useField(datasource: BasicDataSource, query: BasicQuery): AsyncFieldsState {
  const result = useAsync(async () => {
    if (query.measurement == null) {
      return [];
    }

    const { fields } = await datasource.getFieldsList(datasource.database, query.measurement!);

    return fields.map((fields) => ({
      label: fields,
      value: fields,
    }));
  }, [datasource, query]);

  return {
    loading: result.loading,
    fields: result.value ?? [],
    error: result.error,
  };
}

function OnChangeField(selectable: SelectableValue<string>, props: EditorProps) {
  if (!selectable?.value) {
    return;
  }

  props.onChange({
    ...props.query,
    field: selectable.value,
  });
}

export function QueryEditor(props: EditorProps): ReactElement {
  /*
   * In case it isn't obvious, because it really wasn't obvious to me.  Every time the query
   * changes (because we called props.onChange(), this function runs again and generates a
   * whole new snipped of HTML which completely replaces whatever was being displayed before!
   */
  const asyncMeasurementsState = useMeasurements(props.datasource);
  const asyncSeriesState = useSeries(props.datasource, props.query);
  const asyncFieldsState = useField(props.datasource, props.query);

  return (
    <>
      <InlineField label="Measurement" labelWidth={16}>
        <Select
          inputId="editor-measurements"
          options={asyncMeasurementsState.measurements}
          onChange={(selectable) => OnChangeMeasurement(selectable, props)}
          isLoading={asyncMeasurementsState.loading}
          disabled={!!asyncMeasurementsState.error}
          value={props.query.measurement}
        />
      </InlineField>
      <InlineField label="Series" labelWidth={16}>
        <Select
          inputId="editor-series"
          options={asyncSeriesState.series}
          onChange={(selectable) => OnChangeSeries(selectable, props)}
          value={props.query.series}
        />
      </InlineField>
      <InlineField label="Field" labelWidth={16}>
        <Select
          inputId="editor-fields"
          options={asyncFieldsState.fields}
          onChange={(selectable) => OnChangeField(selectable, props)}
          value={props.query.field}
        />
      </InlineField>
    </>
  );
}
