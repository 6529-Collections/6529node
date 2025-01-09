package main

import (
	"github.com/6529-Collections/6529node/internal/config"
	"github.com/6529-Collections/6529node/internal/someservice"
	"go.uber.org/zap"
)

func init() {
	zapConf := zap.Must(zap.NewProduction())
	if config.Get().LogZapMode == "development" {
		zapConf = zap.Must(zap.NewDevelopment())
	}
	zap.ReplaceGlobals(zapConf)
}

func main() {
	zap.L().Debug("Starting 6529-Collections/6529node...")
	someservice.Run()
}
