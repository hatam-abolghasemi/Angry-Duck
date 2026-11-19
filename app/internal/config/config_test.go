package config

import (
	"testing"
	"time"
)

func TestFractionRefusesPercentages(t *testing.T) {
	for v, want := range map[string]float64{
		"0.85": 0.85,
		"1":    1,
		"0":    0,
		"70":   0.70, // meant as 70%: refused
		"-0.1": 0.70,
		"abc":  0.70,
		" 0.5": 0.5,
	} {
		t.Setenv("ANGRYDUCK_TEST_FRACTION", v)
		if got := Fraction("ANGRYDUCK_TEST_FRACTION", 0.70); got != want {
			t.Errorf("Fraction(%q) = %v, want %v", v, got, want)
		}
	}
}

func TestUnparsableValuesFallBack(t *testing.T) {
	t.Setenv("ANGRYDUCK_TEST_INT", "10s")
	if got := Int("ANGRYDUCK_TEST_INT", 7); got != 7 {
		t.Errorf("Int = %d, want the default", got)
	}
	t.Setenv("ANGRYDUCK_TEST_DURATION", "30s")
	if got := Duration("ANGRYDUCK_TEST_DURATION", 5); got != 5*time.Second {
		t.Errorf("Duration = %s, want the default", got)
	}
	t.Setenv("ANGRYDUCK_TEST_BOOL", "yes")
	if got := Bool("ANGRYDUCK_TEST_BOOL", true); !got {
		t.Error("Bool = false, want the default")
	}
	t.Setenv("ANGRYDUCK_TEST_BOOL", "false ")
	if got := Bool("ANGRYDUCK_TEST_BOOL", true); got {
		t.Error("Bool(\"false \") = true: surrounding spaces must not hide the value")
	}
}
