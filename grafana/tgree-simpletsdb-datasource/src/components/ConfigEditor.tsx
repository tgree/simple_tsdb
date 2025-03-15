import React, { ChangeEvent } from 'react';
import { InlineField, Input, SecretInput } from '@grafana/ui';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { MyDataSourceOptions, MySecureJsonData } from '../types';

interface Props extends DataSourcePluginOptionsEditorProps<MyDataSourceOptions, MySecureJsonData> {}

export function ConfigEditor(props: Props) {
  const { onOptionsChange, options } = props;
  const { jsonData, secureJsonFields, secureJsonData } = options;

  const onDatabaseChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      jsonData: {
        ...jsonData,
        database: event.target.value,
      },
    });
  };

  return (
    <>
      <InlineField label="Database" labelWidth={14} interactive tooltip={'Json field returned to frontend'}>
        <Input
          id="config-editor-database"
          onChange={onDatabaseChange}
          value={jsonData.database}
          placeholder="Enter the database, e.g. xdh-n-1000000"
          width={40}
        />
      </InlineField>
    </>
  );
}
