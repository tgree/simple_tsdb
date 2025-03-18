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

  return (
    <>
      <InlineField label="Measurement" grow>
        <Select
          inputId="editor-databases"
          options={asyncMeasurementsState.measurements}
          onChange={onChangeMeasurement}
          isLoading={asyncMeasurementsState.loading}
          disabled={!!asyncMeasurementsState.error}
        />
      </InlineField>
    </>
  );
}
