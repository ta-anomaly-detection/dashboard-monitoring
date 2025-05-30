package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/ta-anomaly-detection/web-server/internal/config"
	"go.uber.org/zap"
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the Echo web server",
	Run: func(cmd *cobra.Command, args []string) {
		viperConfig := config.NewViper()
		log := config.NewLogger(viperConfig)
		db := config.NewDatabase(viperConfig, log.App)
		validate := config.NewValidator(viperConfig)
		app := config.NewEcho(viperConfig)

		config.Bootstrap(&config.BootstrapConfig{
			DB:       db,
			App:      app,
			Log:      log,
			Validate: validate,
			Config:   viperConfig,
		})

		webPort := viperConfig.GetInt("web.port")
		err := app.Start(fmt.Sprintf(":%d", webPort))
		if err != nil {
			log.App.Fatal("Failed to start server", zap.Error(err))
		}
	},
}