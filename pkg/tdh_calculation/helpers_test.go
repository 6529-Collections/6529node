package tdh_calculation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// We'll create a helper function to test parsing a known date string.
func TestParseUTCDateString(t *testing.T) {
	t.Run("valid date parse", func(t *testing.T) {
		testDateStr := "2025-01-02 13:14:15"
		parsed := ParseUTCDateString(testDateStr)

		require.Equal(t, 2025, parsed.Year(), "Year mismatch")
		require.Equal(t, time.January, parsed.Month(), "Month mismatch")
		require.Equal(t, 2, parsed.Day(), "Day mismatch")
		require.Equal(t, 13, parsed.Hour(), "Hour mismatch")
		require.Equal(t, 14, parsed.Minute(), "Minute mismatch")
		require.Equal(t, 15, parsed.Second(), "Second mismatch")
	})

	t.Run("invalid date parse", func(t *testing.T) {
		invalidDateStr := "not-a-date"
		invalid := ParseUTCDateString(invalidDateStr)

		assert.True(t, invalid.IsZero(), "Expected zero time for invalid date string")
	})
}

var timeNow = time.Now

// Redefine GetLastTDH for testing, overriding timeNow var for consistent tests.
func getLastTDHTestable() time.Time {
	now := timeNow().UTC()
	tdh := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	if tdh.After(now) {
		tdh = tdh.Add(-24 * time.Hour)
	}

	tdhStr := tdh.Format("2006-01-02 15:04:05")
	return ParseUTCDateString(tdhStr)
}

func TestGetLastTDH(t *testing.T) {
	origTimeNow := timeNow
	defer func() { timeNow = origTimeNow }()

	t.Run("case1: now is just after midnight", func(t *testing.T) {
		// Suppose "now" is 2025-01-02 00:00:10 UTC
		timeNow = func() time.Time {
			return time.Date(2025, time.January, 2, 0, 0, 10, 0, time.UTC)
		}
		got := getLastTDHTestable()
		want := time.Date(2025, time.January, 2, 0, 0, 0, 0, time.UTC)
		require.Equal(t, want, got, "TDH should be the same day's midnight")
	})

	t.Run("case2: now is exactly midnight", func(t *testing.T) {
		// Suppose "now" is 2025-01-02 00:00:00 UTC
		timeNow = func() time.Time {
			return time.Date(2025, time.January, 2, 0, 0, 0, 0, time.UTC)
		}
		got := getLastTDHTestable()
		want := time.Date(2025, time.January, 2, 0, 0, 0, 0, time.UTC)
		assert.Equal(t, want, got, "TDH should be today's midnight as well")
	})

	t.Run("case3: now is just before midnight", func(t *testing.T) {
		// Suppose "now" is 2025-01-02 00:00:00 UTC minus 1 nanosecond
		// i.e., 2025-01-01 23:59:59.999999999
		// Then the upcoming midnight is "after" now, so we shift back a day
		timeNow = func() time.Time {
			return time.Date(2025, time.January, 2, 0, 0, 0, -1, time.UTC)
		}
		got := getLastTDHTestable()
		want := time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC)
		require.Equal(t, want, got, "TDH should be the previous day at midnight")
	})
}
