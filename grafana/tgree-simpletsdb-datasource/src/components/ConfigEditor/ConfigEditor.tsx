import React, { ReactElement } from 'react';
import { InlineField, Input, SecretInput } from '@grafana/ui';
import type { EditorProps } from './types';
import { useChangeOptions } from './useChangeOptions';
import { useChangeSecureOptions } from './useChangeSecureOptions';

export function ConfigEditor(props: EditorProps): ReactElement {
  const { onOptionsChange, options } = props;
  const { jsonData, secureJsonFields, secureJsonData } = options;
  const onDatabaseFieldChange = useChangeOptions(props, 'database');
  const onHostnameFieldChange = useChangeOptions(props, 'hostname');
  const onUsernameFieldChange = useChangeOptions(props, 'username');
  const onPasswordFieldChange = useChangeSecureOptions(props, 'password');

  const onPasswordFieldReset = () => {
    onOptionsChange({
      ...options,
      secureJsonFields: {
        ...options.secureJsonFields,
        password: false,
      },
      secureJsonData: {
        ...options.secureJsonData,
        password: '',
      },
    });
  };

  return (
    <>
      <InlineField label="Database" labelWidth={14} interactive tooltip={'Database'}>
        <Input
          required
          id="config-editor-database"
          onChange={onDatabaseFieldChange}
          value={jsonData.database}
          placeholder="Enter the database"
          width={40}
        />
      </InlineField>
      <InlineField label="Hostname" labelWidth={14} interactive tooltip={'Hostname'}>
        <Input
          required
          id="config-editor-hostname"
          onChange={onHostnameFieldChange}
          value={jsonData.hostname}
          placeholder="Enter the server hostname:port (normally :4000)"
          width={40}
        />
      </InlineField>
      <InlineField label="Username" labelWidth={14} interactive tooltip={'Username'}>
        <Input
          required
          id="config-editor-username"
          onChange={onUsernameFieldChange}
          value={jsonData.username}
          placeholder="Enter the user name to log in with"
          width={40}
        />
      </InlineField>
      <InlineField label="Password" labelWidth={14} interactive tooltip={'Password'}>
        <SecretInput
          required
          id="config-editor-password"
          isConfigured={secureJsonFields.password}
          value={secureJsonData?.password}
          placeholder="Enter the password to log in with"
          width={40}
          onReset={onPasswordFieldReset}
          onChange={onPasswordFieldChange}
        />
      </InlineField>
    </>
  );
}
