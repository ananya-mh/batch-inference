// This file reads the list of allowed completion windows.
package batches

import (
	"fmt"
	"strings"
	"time"
)

// DefaultCompletionWindows is the only window OpenAI offers.
const DefaultCompletionWindows = "24h"

// ParseCompletionWindows reads a list like "24h,10s" into a map from each
// window, exactly as clients send it, to its length. Tests use short windows
// such as "5s" so expiry can be tested without waiting a day.
func ParseCompletionWindows(list string) (map[string]time.Duration, error) {
	windows := make(map[string]time.Duration)
	for _, window := range strings.Split(list, ",") {
		window = strings.TrimSpace(window)
		if window == "" {
			continue
		}
		length, err := time.ParseDuration(window)
		if err != nil {
			return nil, fmt.Errorf("completion window %q: %w", window, err)
		}
		if length <= 0 {
			return nil, fmt.Errorf("completion window %q must be positive", window)
		}
		windows[window] = length
	}
	if len(windows) == 0 {
		return nil, fmt.Errorf("no completion windows in %q", list)
	}
	return windows, nil
}
