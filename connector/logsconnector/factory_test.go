// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
package logsconnector

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/plogtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/consumer/consumertest"
)

func TestLogsConnector(t *testing.T) {
	type args struct {
		cfg *Config
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "TestLogsConnector",
			args: args{
				cfg: &Config{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, tt.args.cfg.Validate())
			factory := NewFactory()

			sink := &consumertest.LogsSink{}

			conn, err := factory.CreateMetricsToLogs(context.Background(), connectortest.NewNopSettings(), tt.args.cfg, sink)

			require.NoError(t, err)
			require.NotNil(t, conn)
			assert.False(t, conn.Capabilities().MutatesData)

			require.NoError(t, conn.Start(context.Background(), componenttest.NewNopHost()))
			defer func() {
				assert.NoError(t, conn.Shutdown(context.Background()))
			}()

			testMetrics, err := golden.ReadMetrics(filepath.Join("testdata", "metrics", "input.yaml"))
			assert.NoError(t, err)
			assert.NoError(t, conn.ConsumeMetrics(context.Background(), testMetrics))

			allLogs := sink.AllLogs()
			assert.Equal(t, 1, len(allLogs))

			golden.WriteLogs(t, filepath.Join("testdata", "logs", "output.yaml"), allLogs[0])
			expected, err := golden.ReadLogs(filepath.Join("testdata", "logs", "output.yaml"))
			assert.NoError(t, err)
			assert.NoError(t, plogtest.CompareLogs(expected, allLogs[0]))
		})
	}
}
