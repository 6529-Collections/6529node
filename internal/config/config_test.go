package config

import (
	"os"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestGet(t *testing.T) {

	// Set test environment variables
	os.Setenv("LOG_ZAP_MODE", "test_mode")
	os.Setenv("SOME_ELSE", "test_value")
	os.Setenv("PRINT_CONFIGURATION_TO_LOGS", "true")

	// Get config
	cfg := Get()

	// Assert values
	assert.Equal(t, "test_mode", cfg.LogZapMode)
	assert.Equal(t, "test_value", cfg.SomeElse)
	assert.Equal(t, "true", cfg.PrintConfigurationToLogs)

	// Test singleton behavior
	cfg2 := Get()
	assert.Equal(t, cfg, cfg2)
}

func TestLoadConfigWithEnvVars(t *testing.T) {
	// Reset viper
	viper.Reset()

	// Set test environment variables
	os.Setenv("LOG_ZAP_MODE", "debug")
	os.Setenv("SOME_ELSE", "value")
	os.Setenv("PRINT_CONFIGURATION_TO_LOGS", "false")

	cfg := loadConfig()

	assert.Equal(t, "debug", cfg.LogZapMode)
	assert.Equal(t, "value", cfg.SomeElse)
	assert.Equal(t, "false", cfg.PrintConfigurationToLogs)
}

func TestLoadConfigWithConfigFile(t *testing.T) {
	// Reset viper
	viper.Reset()

	// Create temporary config file
	content := []byte(`
LOG_ZAP_MODE=prod
SOME_ELSE=file_value
PRINT_CONFIGURATION_TO_LOGS=true
`)
	err := os.WriteFile("config.env", content, 0644)
	assert.NoError(t, err)
	defer os.Remove("config.env")

	// Clear environment variables to ensure we're reading from file
	os.Unsetenv("LOG_ZAP_MODE")
	os.Unsetenv("SOME_ELSE")
	os.Unsetenv("PRINT_CONFIGURATION_TO_LOGS")

	cfg := loadConfig()

	assert.Equal(t, "prod", cfg.LogZapMode)
	assert.Equal(t, "file_value", cfg.SomeElse)
	assert.Equal(t, "true", cfg.PrintConfigurationToLogs)
}

func TestEnvOverridesConfigFile(t *testing.T) {
	viper.Reset()
	content := []byte(`
	LOG_ZAP_MODE=prod
	SOME_ELSE=file_value
	PRINT_CONFIGURATION_TO_LOGS=true
	`)
	err := os.WriteFile("config.env", content, 0644)
	assert.NoError(t, err)
	defer os.Remove("config.env")

	// Set environment variables that should override file values
	os.Setenv("LOG_ZAP_MODE", "env_override")

	cfg := loadConfig()

	// Environment variable should override file value
	assert.Equal(t, "env_override", cfg.LogZapMode)
	// Other values should come from file
	assert.Equal(t, "file_value", cfg.SomeElse)
	assert.Equal(t, "true", cfg.PrintConfigurationToLogs)
}

func TestMissingConfigFile(t *testing.T) {
	// Reset viper
	viper.Reset()

	// Ensure config file doesn't exist
	os.Remove("config.env")

	// Set environment variables
	os.Setenv("LOG_ZAP_MODE", "fallback")
	os.Setenv("SOME_ELSE", "env_only")
	os.Setenv("PRINT_CONFIGURATION_TO_LOGS", "false")

	// Should not panic when config file is missing
	cfg := loadConfig()

	assert.Equal(t, "fallback", cfg.LogZapMode)
	assert.Equal(t, "env_only", cfg.SomeElse)
	assert.Equal(t, "false", cfg.PrintConfigurationToLogs)
}

// Reset the test environment after each test
func TestMain(m *testing.M) {
	// Run tests
	code := m.Run()

	// Cleanup
	os.Remove("config.env")
	os.Unsetenv("LOG_ZAP_MODE")
	os.Unsetenv("SOME_ELSE")
	os.Unsetenv("PRINT_CONFIGURATION_TO_LOGS")

	os.Exit(code)
}
