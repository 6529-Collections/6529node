package someservice

import (
	"github.com/6529-Collections/6529node/pkg/stringtools"
	"go.uber.org/zap"
)

// Example service function
func Run() {
	zap.L().Info("User logged in",
		zap.String("username", stringtools.ToUpper("johndoe")),
		zap.Int("userid", 123456),
		zap.String("provider", "google"),
	)
}
