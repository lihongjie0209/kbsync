package cli

import (
	"bytes"
	"testing"
)

func TestProgressMode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		mode        string
		logFormat   string
		wantEnabled bool
		wantForced  bool
		wantErr     bool
	}{
		{name: "never", mode: "never", logFormat: "text"},
		{name: "auto non terminal", mode: "auto", logFormat: "text"},
		{name: "auto json", mode: "auto", logFormat: "json"},
		{name: "always", mode: "always", logFormat: "text", wantEnabled: true, wantForced: true},
		{name: "always json rejected", mode: "always", logFormat: "json", wantErr: true},
		{name: "invalid", mode: "sometimes", logFormat: "text", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			enabled, forced, err := progressMode(test.mode, test.logFormat, &bytes.Buffer{})
			if (err != nil) != test.wantErr {
				t.Fatalf("progressMode() error = %v, wantErr %v", err, test.wantErr)
			}
			if enabled != test.wantEnabled || forced != test.wantForced {
				t.Fatalf("progressMode() = (%v, %v), want (%v, %v)", enabled, forced, test.wantEnabled, test.wantForced)
			}
		})
	}
}
