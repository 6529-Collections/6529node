package someservice

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestRun(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	testLogger := zap.New(core)
	zap.ReplaceGlobals(testLogger)

	Run()

	allLogs := logs.All()
	assert.Equal(t, 1, len(allLogs))
	assert.Equal(t, "User logged in", allLogs[0].Message)
	fields := allLogs[0].ContextMap()
	assert.Equal(t, "JOHNDOE", fields["username"])
	assert.Equal(t, int64(123456), fields["userid"])
	assert.Equal(t, "google", fields["provider"])
}
