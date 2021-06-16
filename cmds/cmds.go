package cmds

import (
	"fmt"
	"os"

	"github.com/DataWorkbench/common/utils/buildinfo"
	"github.com/spf13/cobra"

	"github.com/DataWorkbench/jobwatcher/config"
	"github.com/DataWorkbench/jobwatcher/server"
)

var (
	versionFlag bool
)

// root represents the base command when called without any sub commands.
var root = &cobra.Command{
	Use:   "jobwatcher",
	Short: "DataWorkbench Job Watcher",
	Long:  "DataWorkbench Job Watcher",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		if versionFlag {
			fmt.Println(buildinfo.MultiString)
			return
		}
		_ = cmd.Help()
	},
}

// start used to start the service
var start = &cobra.Command{
	Use:   "start",
	Short: "Command to start server",
	Long:  "Command to start server",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		if err := server.Start(); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "start server fail: %v\n", err)
			os.Exit(1)
		}
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the root command.
func Execute() {
	// Add sub command 'start'
	root.AddCommand(start)

	// execute root command
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	// set root command flags
	root.Flags().BoolVarP(
		&versionFlag, "version", "v", false, "show the version",
	)

	// set start command flags
	start.Flags().StringVarP(
		&config.FilePath, "config", "c", "", "path of config file",
	)
}
