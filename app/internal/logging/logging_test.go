package logging

import "testing"

func TestParseLevel(t *testing.T) {
	cases := map[string]Level{
		"debug":    LevelDebug,
		"DEBUG":    LevelDebug,
		"  Debug ": LevelDebug,
		"info":     LevelInfo,
		"":         LevelInfo,
		"bogus":    LevelInfo,
		"warn":     LevelWarn,
		"warning":  LevelWarn,
		"WARN":     LevelWarn,
		"error":    LevelError,
		"ERROR":    LevelError,
	}
	for input, want := range cases {
		if got := ParseLevel(input); got != want {
			t.Errorf("ParseLevel(%q) = %v, want %v", input, got, want)
		}
	}
}

func TestSetLevelFiltersLowerSeverity(t *testing.T) {
	// Restore the package-level state afterwards so this test doesn't leak
	// into others (current is a package var).
	defer SetLevel(LevelInfo)

	SetLevel(LevelWarn)
	if current != LevelWarn {
		t.Fatalf("SetLevel did not update current level")
	}

	// logAt itself is what decides whether to print; we can't easily
	// capture stdlib log output without redirecting it, but we can at
	// least confirm the level comparison logic used by logAt behaves as
	// expected for each level relative to the configured threshold.
	tests := []struct {
		msgLevel   Level
		wantLogged bool
	}{
		{LevelDebug, false},
		{LevelInfo, false},
		{LevelWarn, true},
		{LevelError, true},
	}
	for _, tc := range tests {
		got := tc.msgLevel >= current
		if got != tc.wantLogged {
			t.Errorf("level %v >= current(%v) = %v, want %v", tc.msgLevel, current, got, tc.wantLogged)
		}
	}
}
