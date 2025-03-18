import React, { ReactElement, useCallback } from 'react';
import { InlineField, Select } from '@grafana/ui';
import type { SelectableValue } from '@grafana/data';
import { useMeasurements } from './useMeasurements';
import type { EditorProps } from './types';

export function QueryEditor(props: EditorProps): ReactElement {
  const { datasource } = props;

  const asyncMeasurementsState = useMeasurements(datasource);

  const onChangeMeasurement = useCallback(
    (selectable: SelectableValue<string>) => {
      if (!selectable?.value) {
        return;
      }

      props.onChange({
        ...props.query,
        measurement: selectable.value,
      });
    },
    [props]
  );

  const onChangeSeries = useCallback(
    (selectable: SelectableValue<string>) => {
    },
    []
  );

  const onChangeField = useCallback(
    (selectable: SelectableValue<string>) => {
    },
    []
  );

  return (
    <>
      <InlineField label="Measurement" labelWidth={16}>
        <Select
          inputId="editor-databases"
          options={asyncMeasurementsState.measurements}
          onChange={onChangeMeasurement}
          isLoading={asyncMeasurementsState.loading}
          disabled={!!asyncMeasurementsState.error}
        />
      </InlineField>
      <InlineField label="Series" labelWidth={16}>
        <Select
          inputId="editor-series"
          onChange={onChangeSeries}
          disabled={true}
        />
      </InlineField>
      <InlineField label="Field" labelWidth={16}>
        <Select
          inputId="editor-field"
          onChange={onChangeField}
          disabled={true}
        />
      </InlineField>
    </>
  );
}
