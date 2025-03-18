import React, { ReactElement } from 'react';
import { InlineFieldRow, InlineField, Select } from '@grafana/ui';
import { useMeasurements } from './useMeasurements';
import { useSelectableValue } from './useSelectableValue';
import { useChangeSelectableValue } from './useChangeSelectableValue';
import type { EditorProps } from './types';

export function QueryEditor(props: EditorProps): ReactElement {
  const { datasource, query } = props;

  const { loading, measurements, error } = useMeasurements(datasource);
  const queryType = useSelectableValue(query.queryType);

  const onChangeMeasurement = useChangeSelectableValue(props, {
    propertyName: 'measurement',
    runQuery: true,
  });

  return (
    <>
      <InlineFieldRow>
        <InlineField label="Measurement" grow>
          <Select
            inputId="editor-databases"
            options={measurements}
            onChange={onChangeMeasurement}
            isLoading={loading}
            disabled={!!error}
            value={queryType}
          />
        </InlineField>
      </InlineFieldRow>
    </>
  );
}
