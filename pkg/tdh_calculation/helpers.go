package tdh_calculation

import (
	"time"
)

// ParseUTCDateString replicates parseUTCDateString(...) from TS:
//   const parsedDate = moment.tz(dateString, "YYYY-MM-DD HH:mm:ss", "UTC");
//   return parsedDate.toDate();
func ParseUTCDateString(dateString string) time.Time {
	// The Go equivalent to the moment(...) parse using the "YYYY-MM-DD HH:mm:ss" format in UTC
	layout := "2006-01-02 15:04:05"
	t, err := time.ParseInLocation(layout, dateString, time.UTC)
	if err != nil {
		// The TS code doesn't explicitly throw on parse errors,
		// so we'll return a zero time here to mimic that behavior.
		return time.Time{}
	}
	return t
}

// GetLastTDH replicates getLastTDH(...) from TS:
//   1. now = new Date()
//   2. tdh = new Date( Date.UTC(year, month, day, 0, 0, 0, 0) )
//   3. if (tdh > now) tdh = tdh - 24h
//   4. tdhStr = moment(tdh).tz("UTC").format("YYYY-MM-DD HH:mm:ss")
//   5. return parseUTCDateString(tdhStr)
func GetLastTDH() time.Time {
	now := time.Now().UTC()

	// Step 1 & 2: midnight (00:00:00) of the current UTC day
	tdh := time.Date(
		now.Year(),
		now.Month(),
		now.Day(),
		0, 0, 0, 0,
		time.UTC,
	)

	// Step 3: if tdh is after right now, subtract a day
	if tdh.After(now) {
		tdh = tdh.Add(-24 * time.Hour)
	}

	// Step 4: Format as "YYYY-MM-DD HH:mm:ss" in UTC
	tdhStr := tdh.Format("2006-01-02 15:04:05")

	// Step 5: Re-parse it (mimics the final parseUTCDateString)
	return ParseUTCDateString(tdhStr)
}
