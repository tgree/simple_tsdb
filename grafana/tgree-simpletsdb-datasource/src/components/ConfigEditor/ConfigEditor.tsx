import React, { ReactElement } from 'react';
import { InlineField, Input } from '@grafana/ui';
import type { EditorProps } from './types';
import { useChangeOptions } from './useChangeOptions';

export function ConfigEditor(props: EditorProps): ReactElement {
  const { jsonData } = props.options;
  const onDatabaseFieldChange = useChangeOptions(props, 'database');

  return (
    <>
      <InlineField label="Database" labelWidth={14} interactive tooltip={'Database'}>
        <Input
          id="config-editor-database"
          onChange={onDatabaseFieldChange}
          value={jsonData.database}
          placeholder="Enter the database"
          width={40}
        />
      </InlineField>
    </>
  );
}
