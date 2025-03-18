import { useAsync } from 'react-use';
import type { SelectableValue } from '@grafana/data';
import type { BasicDataSource } from '../../datasource';

type AsyncDatabasesState = {
  loading: boolean;
  databases: Array<SelectableValue<string>>;
  error: Error | undefined;
};

export function useDatabases(datasource: BasicDataSource): AsyncDatabasesState {
  const result = useAsync(async () => {
    const { databases } = await datasource.getDatabaseList();

    return databases.map((database) => ({
      label: database,
      value: database,
    }));
  }, [datasource]);

  return {
    loading: result.loading,
    databases: result.value ?? [],
    error: result.error,
  };
}
