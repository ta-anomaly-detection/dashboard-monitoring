package cmd

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/spf13/cobra"
	"github.com/ta-anomaly-detection/web-server/internal/config"
)

var migrateCmd = &cobra.Command{
	Use:   "migrate [up|down]",
	Short: "Run database migrations using golang-migrate CLI",
	Run: func(cmd *cobra.Command, args []string) {
		viper := config.NewViper()
		dsn := config.BuildDSN(viper, true)
		migrationsPath := "db/migrations"
		if len(args) < 1 {
			fmt.Println("Please specify 'up' or 'down' for migration direction")
			os.Exit(1)
		}
		
		opt := args[0]
		if opt != "up" && opt != "down" {
			fmt.Println("Invalid argument. Use 'up' or 'down'")
			os.Exit(1)
		}


		migrateCmd := exec.Command("migrate", "-database", dsn, "-path", migrationsPath, opt)
		migrateCmd.Stdout = os.Stdout
		migrateCmd.Stderr = os.Stderr

		if err := migrateCmd.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "Migration failed: %v\n", err)
			os.Exit(1)
		}
	},
}