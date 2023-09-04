package cmd

import (
	"fmt"
	"os"

	"github.com/foxglove/go-rosbag/rosbag"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

// chunksCmd represents the chunks command
var chunksCmd = &cobra.Command{
	Use: "chunks",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			dief("requires 1 argument")
		}
		filename := args[0]
		f, err := os.Open(filename)
		if err != nil {
			dief("failed to open input file: %s", err)
		}
		reader, err := rosbag.NewReader(f)
		if err != nil {
			dief("failed to construct reader: %s", err)
		}
		info, err := reader.Info()
		if err != nil {
			dief("failed to read info: %s", err)
		}
		rows := make([][]string, 0, len(info.ChunkInfos)+1)
		rows = append(rows, []string{
			"offset",
			"start",
			"end",
			"connections",
			"messages",
		})
		for _, chunkInfo := range info.ChunkInfos {
			messageCount := uint32(0)
			for _, count := range chunkInfo.Data {
				messageCount += count
			}

			rows = append(rows, []string{
				fmt.Sprintf("%d", chunkInfo.ChunkPos),
				fmt.Sprintf("%d", chunkInfo.StartTime),
				fmt.Sprintf("%d", chunkInfo.EndTime),
				fmt.Sprintf("%d", chunkInfo.Count),
				fmt.Sprintf("%d", messageCount),
			})
		}
		formatTable(os.Stdout, rows, tablewriter.ALIGN_LEFT)
	},
}

func init() {
	listCmd.AddCommand(chunksCmd)
}
