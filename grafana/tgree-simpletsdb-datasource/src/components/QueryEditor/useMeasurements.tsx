import { useAsync } from 'react-use';
import type { SelectableValue } from '@grafana/data';
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
