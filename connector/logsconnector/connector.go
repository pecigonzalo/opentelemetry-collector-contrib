// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

package logsconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/logsconnector"

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

const scopeName = "otelcol/logsconnector"

type connectorImp struct {
	config       Config
	logger       *zap.Logger
	logsConsumer consumer.Logs
}

func newConnector(logger *zap.Logger, config component.Config) (*connectorImp, error) {
	logger.Info("Building logsconnector connector")
	cfg := config.(*Config)

	return &connectorImp{
		config: *cfg,
		logger: logger,
	}, nil
}

// Capabilities implements the consumer interface.
func (c *connectorImp) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{
		MutatesData: false,
	}
}

func (c *connectorImp) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	// var multiError error
	logs := plog.NewLogs()

	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		resourceMetric := md.ResourceMetrics().At(i)
		logResource := logs.ResourceLogs().AppendEmpty()

		resourceMetric.Resource().Attributes().CopyTo(logResource.Resource().Attributes())
		logResource.SetSchemaUrl(resourceMetric.SchemaUrl())

		for j := 0; j < resourceMetric.ScopeMetrics().Len(); j++ {
			scopeMetric := resourceMetric.ScopeMetrics().At(j)
			scopeLog := logResource.ScopeLogs().AppendEmpty()

			scopeLog.SetSchemaUrl(scopeMetric.SchemaUrl())

			for k := 0; k < scopeMetric.Metrics().Len(); k++ {
				m := scopeMetric.Metrics().At(k)

				switch m.Type() {
				case pmetric.MetricTypeSum:
					sum := m.Sum()
					for d := 0; d < sum.DataPoints().Len(); d++ {
						monotonic := sum.IsMonotonic()
						datapoint := sum.DataPoints().At(d)

						var metricType string
						if monotonic {
							metricType = "counter"
						} else {
							metricType = "gauge"
						}

						l := scopeLog.LogRecords().AppendEmpty()
						err := addLogRecordFromNumericDatapoint(
							l,
							datapoint,
							metricType,
							m.Name(),
						)
						if err != nil {
							panic(err)
						}
					}
				case pmetric.MetricTypeGauge:
					gauge := m.Gauge()
					for d := 0; d < gauge.DataPoints().Len(); d++ {
						datapoint := gauge.DataPoints().At(d)

						metricType := m.Type().String()

						l := scopeLog.LogRecords().AppendEmpty()
						err := addLogRecordFromNumericDatapoint(
							l,
							datapoint,
							metricType,
							m.Name(),
						)
						if err != nil {
							panic(err)
						}
					}

				case pmetric.MetricTypeHistogram:
					hist := m.Histogram()
					for d := 0; d < hist.DataPoints().Len(); d++ {
						datapoint := hist.DataPoints().At(d)

						metricType := m.Type().String()

						l := scopeLog.LogRecords().AppendEmpty()
						err := addLogRecordFromHistDatapoint(
							l,
							datapoint,
							metricType,
							m.Name(),
						)
						if err != nil {
							panic(err)
						}
					}
				case pmetric.MetricTypeExponentialHistogram:
					hist := m.ExponentialHistogram()
					for d := 0; d < hist.DataPoints().Len(); d++ {
						datapoint := hist.DataPoints().At(d)

						metricType := m.Type().String()

						l := scopeLog.LogRecords().AppendEmpty()
						err := addLogRecordFromExpHistDatapoint(
							l,
							datapoint,
							metricType,
							m.Name(),
						)
						if err != nil {
							panic(err)
						}
					}
				case pmetric.MetricTypeSummary:
					sum := m.Summary()
					for d := 0; d < sum.DataPoints().Len(); d++ {
						datapoint := sum.DataPoints().At(d)

						metricType := m.Type().String()

						l := scopeLog.LogRecords().AppendEmpty()
						err := addLogRecordFromSummaryDatapoint(
							l,
							datapoint,
							metricType,
							m.Name(),
						)
						if err != nil {
							panic(err)
						}
					}

				case pmetric.MetricTypeEmpty:
				default:
					panic("unexpected pmetric.MetricType")
				}

			}
		}
	}

	return c.logsConsumer.ConsumeLogs(ctx, logs)
}

// Start implements the component.Component interface.
func (c *connectorImp) Start(ctx context.Context, host component.Host) error {
	c.logger.Info("Starting logsconnector connector")

	return nil
}

// Shutdown implements the component.Component interface.
func (c *connectorImp) Shutdown(context.Context) error {
	c.logger.Info("Shutting down logsconnector connector")
	return nil
}

func addLogRecordFromNumericDatapoint(
	logRecord plog.LogRecord,
	datapoint pmetric.NumberDataPoint,
	metricType string,
	metricName string,
) error {
	logRecord.SetSeverityText("INFO")

	datapoint.Attributes().CopyTo(logRecord.Attributes())

	body := generateBody(
		metricName,
		metricType,
	)
	body["value"] = getValueFromNumeric(datapoint)

	err := logRecord.Body().FromRaw(body)
	if err != nil {
		return err
	}
	return nil
}

func addLogRecordFromHistDatapoint(
	logRecord plog.LogRecord,
	datapoint pmetric.HistogramDataPoint,
	metricType string,
	metricName string,
) error {
	logRecord.SetSeverityText("INFO")

	datapoint.Attributes().CopyTo(logRecord.Attributes())

	body := generateBody(
		metricName,
		metricType,
	)

	body["count"] = datapoint.Count()
	if datapoint.HasSum() {
		body["sum"] = datapoint.Sum()
	}
	if datapoint.HasMax() {
		body["max"] = datapoint.Max()
	}
	if datapoint.HasMin() {
		body["min"] = datapoint.Min()
	}

	buckets := []any{}
	for i := 0; i < datapoint.BucketCounts().Len(); i++ {
		bucket := datapoint.BucketCounts().At(i)
		buckets = append(buckets, bucket)
	}
	body["buckets"] = buckets

	bounds := []any{}
	for i := 0; i < datapoint.ExplicitBounds().Len(); i++ {
		bound := datapoint.ExplicitBounds().At(i)
		bounds = append(bounds, bound)
	}
	body["bounds"] = bounds

	err := logRecord.Body().FromRaw(body)
	if err != nil {
		return err
	}
	return nil
}

func addLogRecordFromExpHistDatapoint(
	logRecord plog.LogRecord,
	datapoint pmetric.ExponentialHistogramDataPoint,
	metricType string,
	metricName string,
) error {
	logRecord.SetSeverityText("INFO")

	datapoint.Attributes().CopyTo(logRecord.Attributes())

	body := generateBody(
		metricName,
		metricType,
	)

	body["count"] = datapoint.Count()
	if datapoint.HasSum() {
		body["sum"] = datapoint.Sum()
	}
	if datapoint.HasMax() {
		body["max"] = datapoint.Max()
	}
	if datapoint.HasMin() {
		body["min"] = datapoint.Min()
	}

	body["scale"] = datapoint.Scale()
	body["zero_threshold"] = datapoint.ZeroThreshold()
	body["zero_count"] = datapoint.ZeroCount()

	negatives := []any{}
	for i := 0; i < datapoint.Negative().BucketCounts().Len(); i++ {
		bucket := datapoint.Negative().BucketCounts().At(i)
		negatives = append(negatives, bucket)
	}
	body["negatives"] = negatives

	positives := []any{}
	for i := 0; i < datapoint.Positive().BucketCounts().Len(); i++ {
		bucket := datapoint.Positive().BucketCounts().At(i)
		positives = append(positives, bucket)
	}
	body["positives"] = positives

	err := logRecord.Body().FromRaw(body)
	if err != nil {
		return err
	}
	return nil
}

func addLogRecordFromSummaryDatapoint(
	logRecord plog.LogRecord,
	datapoint pmetric.SummaryDataPoint,
	metricType string,
	metricName string,
) error {
	logRecord.SetSeverityText("INFO")

	datapoint.Attributes().CopyTo(logRecord.Attributes())

	body := generateBody(
		metricName,
		metricType,
	)
	body["count"] = datapoint.Count()
	body["sum"] = datapoint.Sum()

	quantiles := []any{}
	for i := 0; i < datapoint.QuantileValues().Len(); i++ {
		quantile := datapoint.QuantileValues().At(i)
		quantiles = append(quantiles, quantile)
	}
	body["quantile"] = quantiles

	err := logRecord.Body().FromRaw(body)
	if err != nil {
		return err
	}
	return nil
}

func generateBody(n string, t string) map[string]any {
	body := make(map[string]any)

	body["message"] = "metric"
	body["name"] = n
	body["type"] = t

	return body
}

func getValueFromNumeric(dp pmetric.NumberDataPoint) any {
	var result any
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeDouble:
		result = fmt.Sprintf("%v", dp.DoubleValue())
	case pmetric.NumberDataPointValueTypeInt:
		result = fmt.Sprintf("%v", dp.IntValue())
	case pmetric.NumberDataPointValueTypeEmpty:
		result = "0" // Do we want empty as 0?
	default:
		panic("unexpected pmetric.NumberDataPointValueType")
	}

	return result
}
