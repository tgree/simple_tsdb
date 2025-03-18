import React, { ReactElement } from 'react';
import { InlineField, Select } from '@grafana/ui';
import { useMeasurements } from './useMeasurements';
import { useChangeSelectableValue } from './useChangeSelectableValue';
import type { EditorProps } from './types';

export function QueryEditor(props: EditorProps): ReactElement {
  const { datasource } = props;

  const asyncMeasurementsState = useMeasurements(datasource);

  const onChangeMeasurement = useChangeSelectableValue(props, {
    propertyName: 'measurement',
    runQuery: true,
  });

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
