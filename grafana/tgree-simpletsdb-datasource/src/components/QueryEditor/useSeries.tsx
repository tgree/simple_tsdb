import { useAsync } from 'react-use';
import type { SelectableValue } from '@grafana/data';
import type { BasicDataSource } from '../../datasource';

type AsyncSeriesState = {
  loading: boolean;
  series: Array<SelectableValue<string>>;
  error: Error | undefined;
};

export function useSeries(datasource: BasicDataSource, measurement: string): AsyncSeriesState {
  const result = useAsync(async () => {
    const { series } = await datasource.getSeries(datasource.database, measurement);

    return series.map((s) => ({
      label: s,
      value: s,
    }));
  }, [datasource]);

  return {
    loading: result.loading,
    measurements: result.value ?? [],
    error: result.error,
  };
}
