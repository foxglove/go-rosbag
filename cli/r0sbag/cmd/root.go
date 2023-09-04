package cmd

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/spf13/cobra"

	"github.com/spf13/viper"
)

var pprofProfile bool
var profileCloser func()

func makeProfileCloser(pprofProfile bool) func() {
	if !pprofProfile {
		return func() {}
	}

	cpu := "rosbag-cpu.prof"
	mem := "rosbag-mem.prof"
	block := "rosbag-block.prof"
	memprof, err := os.Create(mem)
	if err != nil {
		log.Fatal(err)
	}
	cpuprof, err := os.Create(cpu)
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(cpuprof)
	runtime.SetBlockProfileRate(100e6)
	blockProfile, err := os.Create(block)
	if err != nil {
		log.Fatal(err)
	}
	return func() {
		pprof.StopCPUProfile()
		cpuprof.Close()

		pprof.WriteHeapProfile(memprof)
		memprof.Close()

		pprof.Lookup("block").WriteTo(blockProfile, 0)
		blockProfile.Close()
		fmt.Fprintf(os.Stderr, "Wrote profiles to %s, %s, and %s\n", cpu, mem, block)
	}
}

func dief(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

var cfgFile string

var rootCmd = &cobra.Command{
	Use:   "r0sbag",
	Short: "A brief description of your application",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		profileCloser = makeProfileCloser(pprofProfile)
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		profileCloser()
	},
}

func Execute() {
	cobra.CheckErr(rootCmd.Execute())
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.bag.yaml)")
	rootCmd.PersistentFlags().BoolVar(&pprofProfile, "pprof", false, "Record profiles of command execution.")

	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigName(".bag")
	}

	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err == nil {
		fmt.Fprintln(os.Stderr, "Using config file:", viper.ConfigFileUsed())
	}
}
