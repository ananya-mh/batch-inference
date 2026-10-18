// This file holds the fake model's settings and reads them from environment
// variables.
package fakemodel

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

// Config controls how the fake model behaves.
type Config struct {
	// Each request waits a random time between MinDelay and MaxDelay, to
	// act like a model that is busy generating text.
	MinDelay time.Duration
	MaxDelay time.Duration

	// OutputWords is how many words each reply contains.
	OutputWords int

	// ErrorRate is the chance (0 to 1) that a request fails with HTTP 500.
	ErrorRate float64

	// CrashRate is the chance (0 to 1) that the server cuts the connection
	// halfway through sending a reply, like a model server that died.
	CrashRate float64
}

// DefaultConfig returns settings that look like a real, slightly unreliable
// model server, so normal development exercises the failure paths too.
func DefaultConfig() Config {
	return Config{
		MinDelay:    50 * time.Millisecond,
		MaxDelay:    500 * time.Millisecond,
		OutputWords: 20,
		ErrorRate:   0.02,
		CrashRate:   0.01,
	}
}

// Validate returns an error if the settings make no sense.
func (c Config) Validate() error {
	if c.MinDelay < 0 || c.MaxDelay < c.MinDelay {
		return fmt.Errorf("delays must satisfy 0 <= min (%s) <= max (%s)", c.MinDelay, c.MaxDelay)
	}
	if c.OutputWords < 1 {
		return fmt.Errorf("output words must be at least 1, got %d", c.OutputWords)
	}
	if c.ErrorRate < 0 || c.ErrorRate > 1 {
		return fmt.Errorf("error rate must be between 0 and 1, got %v", c.ErrorRate)
	}
	if c.CrashRate < 0 || c.CrashRate > 1 {
		return fmt.Errorf("crash rate must be between 0 and 1, got %v", c.CrashRate)
	}
	// Each request rolls one random number to pick crash, error, or success,
	// so the two chances together cannot be more than 100%.
	if c.ErrorRate+c.CrashRate > 1 {
		return errors.New("error rate plus crash rate must not be more than 1")
	}
	return nil
}

// ConfigFromEnv starts from DefaultConfig and overrides each setting whose
// environment variable is set. getenv is normally os.Getenv; tests pass a
// map lookup instead.
//
// Variables: FAKE_MODEL_MIN_DELAY and FAKE_MODEL_MAX_DELAY (like "50ms"),
// FAKE_MODEL_OUTPUT_WORDS, FAKE_MODEL_ERROR_RATE, FAKE_MODEL_CRASH_RATE.
func ConfigFromEnv(getenv func(string) string) (Config, error) {
	config := DefaultConfig()

	if err := readDuration(getenv, "FAKE_MODEL_MIN_DELAY", &config.MinDelay); err != nil {
		return Config{}, err
	}
	if err := readDuration(getenv, "FAKE_MODEL_MAX_DELAY", &config.MaxDelay); err != nil {
		return Config{}, err
	}
	if err := readInt(getenv, "FAKE_MODEL_OUTPUT_WORDS", &config.OutputWords); err != nil {
		return Config{}, err
	}
	if err := readFloat(getenv, "FAKE_MODEL_ERROR_RATE", &config.ErrorRate); err != nil {
		return Config{}, err
	}
	if err := readFloat(getenv, "FAKE_MODEL_CRASH_RATE", &config.CrashRate); err != nil {
		return Config{}, err
	}

	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

// readDuration sets *target from the variable name, if it is set.
func readDuration(getenv func(string) string, name string, target *time.Duration) error {
	value := getenv(name)
	if value == "" {
		return nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	*target = parsed
	return nil
}

// readInt sets *target from the variable name, if it is set.
func readInt(getenv func(string) string, name string, target *int) error {
	value := getenv(name)
	if value == "" {
		return nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	*target = parsed
	return nil
}

// readFloat sets *target from the variable name, if it is set.
func readFloat(getenv func(string) string, name string, target *float64) error {
	value := getenv(name)
	if value == "" {
		return nil
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	*target = parsed
	return nil
}
