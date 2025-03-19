import React, { ReactElement } from 'react';
import { useAsync } from 'react-use';
import { InlineField, Select } from '@grafana/ui';
import type { SelectableValue } from '@grafana/data';
import type { EditorProps } from './types';
import type { BasicDataSource } from '../../datasource';

type AsyncMeasurementsState = {
  loading: boolean;
  measurements: Array<SelectableValue<string>>;
  error: Error | undefined;
};

export function useMeasurements(datasource: BasicDataSource): AsyncMeasurementsState {
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

function OnChangeSeries(selectable: SelectableValue<string>, props: EditorProps) {
}

function OnChangeField(selectable: SelectableValue<string>, props: EditorProps) {
}

export function QueryEditor(props: EditorProps): ReactElement {
  const asyncMeasurementsState = useMeasurements(props.datasource);

  return (
    <>
      <InlineField label="Measurement" labelWidth={16}>
        <Select
          inputId="editor-measurements"
          options={asyncMeasurementsState.measurements}
          onChange={(selectable) => OnChangeMeasurement(selectable, props)}
          isLoading={asyncMeasurementsState.loading}
          disabled={!!asyncMeasurementsState.error}
        />
      </InlineField>
      <InlineField label="Series" labelWidth={16}>
        <Select
          inputId="editor-series"
          options={[] as Array<SelectableValue<string>>}
          onChange={(selectable) => OnChangeSeries(selectable, props)}
          disabled={true}
        />
      </InlineField>
      <InlineField label="Field" labelWidth={16}>
        <Select
          inputId="editor-fields"
          options={[] as Array<SelectableValue<string>>}
          onChange={(selectable) => OnChangeField(selectable, props)}
          disabled={true}
        />
      </InlineField>
    </>
  );
}
