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
	LogZapMode                     string `mapstructure:"LOG_ZAP_MODE"`
	PrintConfigurationToLogs       string `mapstructure:"PRINT_CONFIGURATION_TO_LOGS"`
	EthereumNodeUrl                string `mapstructure:"ETHEREUM_NODE_URL"`
	TdhTransferWatcherMaxChunkSize uint64 `mapstructure:"TDH_TRANSFER_WATCHER_MAX_CHUNK_SIZE"`
}

var lock = &sync.Mutex{}
var config *Config

var Get = get

func get() Config {
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

func loadConfig() Config {
	viperAddConfigFile()
	viperAddEnv()
	cfg := initializeCfg()
	debugConfig(cfg)
	return cfg
}

func viperAddConfigFile() {
	viper.AddConfigPath(".")
	viper.SetConfigName("config")
	viper.SetConfigType("env")
}

func viperAddEnv() {
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
}

func initializeCfg() Config {
	var cfg Config
	err := viper.ReadInConfig()
	if err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
		} else {
			panic(fmt.Sprintf("fatal error reading config file: %v", err))
		}
	}

	err = viper.Unmarshal(&cfg)
	if err != nil {
		panic(fmt.Sprintf("error unmarshaling config: %v", err))
	}
	return cfg
}

func debugConfig(cfg Config) {
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
}
