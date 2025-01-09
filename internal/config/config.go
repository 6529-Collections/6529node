package config

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"sync"

	"github.com/spf13/viper"
)

type Config struct {
	LogZapMode               string `mapstructure:"LOG_ZAP_MODE"`
	SomeElse                 string `mapstructure:"SOME_ELSE"`
	PrintConfigurationToLogs string `mapstructure:"PRINT_CONFIGURATION_TO_LOGS"`
}

func loadConfig() Config {
	// Configure config file
	var cfg Config
	viper.AddConfigPath(".")
	viper.SetConfigName("config")
	viper.SetConfigType("env")

	// Make viper env-aware
	viper.AutomaticEnv()

	// This makes sure that all envs are binded even if they are not represented in config file (https://github.com/spf13/viper/issues/584)
	valueOfConfig := reflect.ValueOf(&Config{}).Elem()
	fieldsOfConfig := reflect.TypeOf(&Config{}).Elem()
	for i := 0; i < valueOfConfig.NumField(); i++ {
		field, _ := fieldsOfConfig.FieldByName(valueOfConfig.Type().Field(i).Name)
		mapStructureVal := field.Tag.Get("mapstructure")
		err := viper.BindEnv(mapStructureVal)
		if err != nil {
			panic(fmt.Sprintf("Error binding env val '%v': %v", mapStructureVal, err))
		}

	}

	// Actually read the config file
	err := viper.ReadInConfig()
	if err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// It's okay if the file is missing; just rely on environment variables
		} else {
			// Some other error (parsing error, permission error, etc.)
			panic(fmt.Sprintf("fatal error reading config file: %v", err))
		}
	}

	// Unmarshal the loaded config into our struct
	err = viper.Unmarshal(&cfg)
	if err != nil {
		panic(fmt.Sprintf("error unmarshaling config: %v", err))
	}

	// Print config to console as JSON if required. This is disabled by default as conf might include secrets.
	if cfg.PrintConfigurationToLogs == "true" {
		b, err := json.Marshal(cfg)
		var result string
		if err != nil {
			result = "[FAILED TO CONVERT CONF TO STRING]"
		} else {
			result = string(b)
		}
		log.Printf("[APP CONFIGURATION]: %v\n", result)
	}

	return cfg
}

var lock = &sync.Mutex{}
var config *Config

// Makes sure Config is initialised only once and can't be mutated externally
func Get() Config {
	if config == nil {
		lock.Lock()
		defer lock.Unlock()
		if config == nil {
			c := loadConfig()
			config = &c
		}
	}
	return *config
}
