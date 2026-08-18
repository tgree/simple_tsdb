import React, { ReactElement } from 'react';
import { css } from '@emotion/css';
import { useAsync } from 'react-use';
import { InlineField, Select, AutoSizeInput, Stack, InlineLabel } from '@grafana/ui';
import type { SelectableValue } from '@grafana/data';
import { getTemplateSrv } from '@grafana/runtime';
import type { EditorProps } from './types';
import type { BasicDataSource } from '../../datasource';
import type { BasicQuery } from '../../types';

type AsyncMeasurementsState = {
  loading: boolean;
  measurements: Array<SelectableValue<string>>;
  error: Error | undefined;
};

function useMeasurements(datasource: BasicDataSource): AsyncMeasurementsState {
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

function OnChangeMeasurement(selectable: SelectableValue<string>, props: EditorProps) {
  if (!selectable?.value) {
    return;
  }

  props.onChange({
    ...props.query,
    measurement: selectable.value,
  });
}

type AsyncSeriesState = {
  loading: boolean;
  series: Array<SelectableValue<string>>;
  error: Error | undefined;
};

function useSeries(datasource: BasicDataSource, query: BasicQuery): AsyncSeriesState {
  const result = useAsync(async () => {
    if (query.measurement == null) {
      return [];
    }

    const variables = getTemplateSrv().getVariables();
    const { series } = await datasource.getSeriesList(datasource.database, query.measurement!);

    return [
        ...variables.map((v) => ({
          label: '$' + v.name,
          value: '$' + v.name,
        })),
        ...series.map((s) => ({
          label: s,
          value: s,
        })),
    ];
  }, [datasource, query]);

  return {
    loading: result.loading,
    series: result.value ?? [],
    error: result.error,
  };
}

function OnChangeSeries(selectable: SelectableValue<string>, props: EditorProps) {
  if (!selectable?.value) {
    return;
  }

  props.onChange({
    ...props.query,
    series: selectable.value,
  });
}

type AsyncFieldsState = {
  loading: boolean;
  fields: Array<SelectableValue<string>>;
  error: Error | undefined;
};

function useField(datasource: BasicDataSource, query: BasicQuery): AsyncFieldsState {
  const result = useAsync(async () => {
    if (query.measurement == null) {
      return [];
    }

    const { fields } = await datasource.getFieldsList(datasource.database, query.measurement!);

    return fields.map((fields) => ({
      label: fields,
      value: fields,
    }));
  }, [datasource, query]);

  return {
    loading: result.loading,
    fields: result.value ?? [],
    error: result.error,
  };
}

function OnChangeField(selectable: SelectableValue<string>, props: EditorProps) {
  if (!selectable?.value) {
    return;
  }

  props.onChange({
    ...props.query,
    field: selectable.value,
  });
}

function OnChangeAlias(alias: string, props: EditorProps) {
  props.onChange({
    ...props.query,
    alias: alias,
  });
}

function OnChangeZoom(selectable: SelectableValue<string>, props: EditorProps) {
  if (!selectable?.value) {
    return;
  }

  props.onChange({
    ...props.query,
    zoom: selectable.value,
  });
}

function OnChangeTransform(selectable: SelectableValue<string>, props: EditorProps) {
  if (!selectable?.value) {
    return;
  }

  props.onChange({
    ...props.query,
    transform: selectable.value,
  });
}

function OnChangeQueryType(selectable: SelectableValue<string>, props: EditorProps) {
  if (!selectable?.value) {
    return;
  }

  props.onChange({
    ...props.query,
    querytype: selectable.value,
  });
}

function OnChangeEquation(equation: string, props: EditorProps) {
  props.onChange({
    ...props.query,
    equation: equation,
  });
}

export function QueryEditor(props: EditorProps): ReactElement {
  /*
   * In case it isn't obvious, because it really wasn't obvious to me.  Every time the query
   * changes (because we called props.onChange(), this function runs again and generates a
   * whole new snippet of HTML which completely replaces whatever was being displayed before!
   */
  const asyncMeasurementsState = useMeasurements(props.datasource);
  const asyncSeriesState = useSeries(props.datasource, props.query);
  const asyncFieldsState = useField(props.datasource, props.query);
  const querytype = props.query.querytype ?? "SELECT";
  const equation = props.query.equation ?? "";

  const zeroMarginClass = css`
    margin-right: 0px !important;
  `;

  if (querytype == "SELECT") {
    return (
      <>
        <Stack direction="column" gap={0.5}>
          <Stack gap={0.5}>
              <Select
                inputId="editor-querytype"
                options={[{label: "SELECT", value: "SELECT"},
                          {label: "COMPUTE", value: "COMPUTE"},
                          ]}
                onChange={(selectable) => OnChangeQueryType(selectable, props)}
                value={"SELECT"}
                width="auto"
              />
              <Select
                inputId="editor-fields"
                options={asyncFieldsState.fields}
                onChange={(selectable) => OnChangeField(selectable, props)}
                value={props.query.field}
                width="auto"
              />
          </Stack>
          <Stack alignItems="center" gap={0.5}>
            <InlineLabel width="auto" className={zeroMarginClass}>
              FROM
            </InlineLabel>
              <Select
                inputId="editor-measurements"
                options={asyncMeasurementsState.measurements}
                onChange={(selectable) => OnChangeMeasurement(selectable, props)}
                isLoading={asyncMeasurementsState.loading}
                disabled={!!asyncMeasurementsState.error}
                value={props.query.measurement}
                width="auto"
              />
              <Select
                inputId="editor-series"
                options={asyncSeriesState.series}
                onChange={(selectable) => OnChangeSeries(selectable, props)}
                value={props.query.series}
                width="auto"
              />
          </Stack>
          <Stack>
            <InlineField label="ZOOM">
              <Select
                inputId="editor-zoom"
                options={[{label: "Mean", value: "Mean"},
                          {label: "Min/Max", value: "Min/Max"},
                          ]}
                onChange={(selectable) => OnChangeZoom(selectable, props)}
                value={props.query.zoom}
                width="auto"
              />
            </InlineField>
            <InlineField label="TRANSFORM">
              <Select
                inputId="editor-transform"
                options={[{label: "None", value: "None"},
                          {label: "Tare", value: "Tare"},
                          {label: "Difference", value: "Difference"},
                          {label: "Derivative (sec)", value: "Derivative (sec)"},
                          {label: "Derivative (min)", value: "Derivative (min)"},
                          {label: "Derivative (hour)", value: "Derivative (hour)"},
                          ]}
                onChange={(selectable) => OnChangeTransform(selectable, props)}
                value={props.query.transform}
                width="auto"
              />
            </InlineField>
            <InlineField label="ALIAS">
              <AutoSizeInput
                onChange={(event) => OnChangeAlias(event.currentTarget.value, props)}
                value={props.query.alias}
              />
            </InlineField>
          </Stack>
        </Stack>
      </>
    );
  }

  return (
    <>
      <Stack direction="column" gap={0.5}>
        <Stack gap={0.5}>
          <Select
            inputId="editor-querytype"
            options={[{label: "SELECT", value: "SELECT"},
                      {label: "COMPUTE", value: "COMPUTE"},
                      ]}
            onChange={(selectable) => OnChangeQueryType(selectable, props)}
            value={"COMPUTE"}
            width="auto"
          />
          <AutoSizeInput
            onChange={(event) => OnChangeEquation(event.currentTarget.value, props)}
            value={equation}
          />
        </Stack>
        <Stack alignItems="center" gap={0.5}>
          <InlineLabel width="auto" className={zeroMarginClass}>
            FROM
          </InlineLabel>
            <Select
              inputId="editor-measurements"
              options={asyncMeasurementsState.measurements}
              onChange={(selectable) => OnChangeMeasurement(selectable, props)}
              isLoading={asyncMeasurementsState.loading}
              disabled={!!asyncMeasurementsState.error}
              value={props.query.measurement}
              width="auto"
            />
            <Select
              inputId="editor-series"
              options={asyncSeriesState.series}
              onChange={(selectable) => OnChangeSeries(selectable, props)}
              value={props.query.series}
              width="auto"
            />
        </Stack>
        <Stack>
          <InlineField label="ZOOM">
            <Select
              inputId="editor-zoom"
              options={[{label: "Mean", value: "Mean"},
                        {label: "Min/Max", value: "Min/Max"},
                        ]}
              onChange={(selectable) => OnChangeZoom(selectable, props)}
              value={props.query.zoom}
              width="auto"
            />
          </InlineField>
          <InlineField label="TRANSFORM">
            <Select
              inputId="editor-transform"
              options={[{label: "None", value: "None"},
                        {label: "Tare", value: "Tare"},
                        {label: "Difference", value: "Difference"},
                        {label: "Derivative (sec)", value: "Derivative (sec)"},
                        {label: "Derivative (min)", value: "Derivative (min)"},
                        {label: "Derivative (hour)", value: "Derivative (hour)"},
                        ]}
              onChange={(selectable) => OnChangeTransform(selectable, props)}
              value={props.query.transform}
              width="auto"
            />
          </InlineField>
          <InlineField label="ALIAS">
            <AutoSizeInput
              onChange={(event) => OnChangeAlias(event.currentTarget.value, props)}
              value={props.query.alias}
            />
          </InlineField>
        </Stack>
      </Stack>
    </>
  );
}
