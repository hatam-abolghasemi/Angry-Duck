// Package logging provides minimal leveled logging (DEBUG/INFO/WARN/ERROR)
// on top of the standard library's log package. It exists because the
// standard logger has no concept of level, which made it impossible to
// separate "here's what happened" noise from "here's a decision worth your
// attention" signal — a real operational problem once GC's per-check
// reasoning needed to be visible without drowning out everything else.
package logging

import (
	"log"
	"strings"
)

// Level controls which messages actually get printed. Debug is the most
// verbose; Error is the least.
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

var current = LevelInfo

// SetLevel sets the minimum level that will actually be printed. Call this
// once at startup after reading the LOG_LEVEL config value.
func SetLevel(l Level) {
	current = l
}

// ParseLevel converts a config string ("debug", "info", "warn"/"warning",
// "error") into a Level, case-insensitively. Anything unrecognized
// (including empty) falls back to LevelInfo rather than erroring, since a
// typo'd log level shouldn't crash the process.
func ParseLevel(s string) Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return LevelDebug
	case "warn", "warning":
		return LevelWarn
	case "error":
		return LevelError
	default:
		return LevelInfo
	}
}

// Debugf logs fine-grained, high-volume detail: per-item reasoning inside a
// loop, computed intermediate values, anything you'd only want when
// actively troubleshooting. Off by default (LOG_LEVEL=info).
func Debugf(format string, args ...interface{}) { logAt(LevelDebug, "DEBUG", format, args...) }

// Infof logs normal operational events and decisions: a tick completed, an
// image was ordered pulled, a report was pushed. Visible by default.
func Infof(format string, args ...interface{}) { logAt(LevelInfo, "INFO", format, args...) }

// Warnf logs decisions with real consequences worth extra attention without
// being an outright failure: an image is being removed, a request was
// rejected, a retry is happening.
func Warnf(format string, args ...interface{}) { logAt(LevelWarn, "WARN", format, args...) }

// Errorf logs failures: a command failed, a network call errored out, an
// operation could not complete.
func Errorf(format string, args ...interface{}) { logAt(LevelError, "ERROR", format, args...) }

func logAt(l Level, tag, format string, args ...interface{}) {
	if l < current {
		return
	}
	log.Printf("["+tag+"] "+format, args...)
}
