package main

import (
	"github.com/6529-Collections/6529node/internal/config"
	"github.com/6529-Collections/6529node/pkg/tdh_calculation"
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
    zap.L().Info("Starting 'tdh_calculation' task...")

    // Placeholder call to a function in the tdh package.
    // We'll flesh this out as we go along.
    tdh_calculation.RunTdhCalculation()
}
