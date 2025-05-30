package cmd

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/spf13/cobra"
)

var createCmd = &cobra.Command{
	Use:   "create [name]",
	Short: "Create a new migration file",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		name := args[0]
		migrateCmd := exec.Command("migrate", "create", "-ext", "sql", "-dir", "db/migrations", name)
		migrateCmd.Stdout = os.Stdout
		migrateCmd.Stderr = os.Stderr
		err := migrateCmd.Run()
		if err != nil {
			fmt.Println("Failed to create migration:", err)
			os.Exit(1)
		}
	},
}