package cmd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/foxglove/go-rosbag/rosbag"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

var (
	displayCaveats bool
)

const (
	infoLayout = "Jan 02 2006 15:04:05.00"
)

func digits(n int64) int {
	if n == 0 {
		return 1
	}
	count := 0
	for n != 0 {
		n = n / 10
		count++
	}
	return count
}

func humanBytes(numBytes int64) string {
	prefixes := []string{"B", "KB", "MB", "GB"}
	displayedValue := float64(numBytes)
	prefixIndex := 0
	for ; displayedValue > 1024 && prefixIndex < len(prefixes); prefixIndex++ {
		displayedValue = displayedValue / 1024
	}
	return fmt.Sprintf("%.1f %s", displayedValue, prefixes[prefixIndex])
}

func formatDuration(d time.Duration) string {
	scale := 100 * time.Second
	for scale > d {
		scale = scale / 10
	}
	return d.Round(scale / 100).String()
}

func formatTable(w io.Writer, rows [][]string, align int) {
	tw := tablewriter.NewWriter(w)
	tw.SetBorder(false)
	tw.SetAutoWrapText(false)
	tw.SetAlignment(align)
	tw.SetHeaderAlignment(align)
	tw.SetColumnSeparator("")
	tw.AppendBulk(rows)
	tw.Render()
}

func printInfo(
	w io.Writer,
	rs io.ReadSeeker,
	path string,
	size int64,
	displayCaveats bool,
	info *rosbag.Info,
) {
	start := int64(info.MessageStartTime)
	end := int64(info.MessageEndTime)
	duration := time.Duration(end - start)
	startTime := time.Unix(start/1e9, start%1e9)
	endTime := time.Unix(end/1e9, end%1e9)
	startFormat := startTime.Format(infoLayout)
	endFormat := endTime.Format(infoLayout)

	header := [][]string{
		{"path:", path},
		{"version:", "2.0"},
		{"duration:", formatDuration(duration)},
		{"start:", fmt.Sprintf("%s (%d.%02.0f)", startFormat, start/1e9, math.Round(100*float64(start%1e9)/float64(1e9)))},
		{"end:", fmt.Sprintf("%s (%d.%02.0f)", endFormat, end/1e9, math.Round(100*float64(end%1e9)/float64(1e9)))},
		{"size:", humanBytes(size)},
		{"messages:", fmt.Sprintf("%d", info.MessageCount)},
	}

	/*
		Avoid visiting every chunk to juice the speed numbers

		Pick a representative chunk and assume all chunks are like it:
		* the first is most likely polluted with connections
		* the last may be partial

		So, if we have at least three, take the second to last and if we
		have fewer than three, take the first.
	*/

	if chunkCount := len(info.ChunkInfos); chunkCount > 0 {
		var representativeChunk *rosbag.ChunkInfo
		if chunkCount >= 3 {
			representativeChunk = info.ChunkInfos[chunkCount-2]
		} else {
			representativeChunk = info.ChunkInfos[0]
		}
		_, err := rs.Seek(int64(representativeChunk.ChunkPos), io.SeekStart)
		if err != nil {
			dief("failed to seek to chunk: %v", err)
		}
		length := make([]byte, 4)
		_, err = io.ReadFull(rs, length)
		if err != nil {
			dief("failed to read chunk header length: %v", err)
		}
		chunkheader := make([]byte, binary.LittleEndian.Uint32(length))
		_, err = io.ReadFull(rs, chunkheader)
		if err != nil {
			dief("failed to read chunk header: %v", err)
		}
		compressionHeader, err := rosbag.GetHeaderValue(chunkheader, "compression")
		if err != nil {
			dief("failed to read compression: %s", err)
		}
		sizeHeader, err := rosbag.GetHeaderValue(chunkheader, "size")
		if err != nil {
			dief("failed to read size: %s", err)
		}
		_, err = rs.Read(length)
		if err != nil {
			dief("failed to read chunk length: %v", err)
		}
		compressedSize := binary.LittleEndian.Uint32(length)
		chunkDecompressedSize := int(binary.LittleEndian.Uint32(sizeHeader))
		compression := string(compressionHeader)
		chunkCount := len(info.ChunkInfos)

		// estimates
		uncompressedVolume := int64(chunkCount) * int64(chunkDecompressedSize)
		compressedVolume := min(int64(chunkCount)*int64(compressedSize), size)
		uncompressedRate := float64(uncompressedVolume) / duration.Seconds()
		compressedRate := float64(compressedVolume) / duration.Seconds()
		compressionPct := 100 * float64(compressedVolume) / float64(uncompressedVolume)

		maybeCaveat := func(s string) string {
			if displayCaveats {
				return "*" + s
			}
			return s
		}

		header = append(header, [][]string{
			{maybeCaveat("compression") + ":", fmt.Sprintf(
				"%s [%d/%d chunks; %.2f%%]",
				compression,
				chunkCount, chunkCount,
				compressionPct,
			)},
			{maybeCaveat("uncompressed") + ":", fmt.Sprintf(
				"%s @ %s/s",
				humanBytes(uncompressedVolume),
				humanBytes(int64(uncompressedRate)),
			)},
			{maybeCaveat("compressed") + ":", fmt.Sprintf(
				"%s @ %s/s (%.2f%%)",
				humanBytes(compressedVolume),
				humanBytes(int64(compressedRate)),
				compressionPct,
			)},
		}...)
	}

	/*
		end trickery
	*/

	formatTable(w, header, tablewriter.ALIGN_LEFT)

	connIDs := make([]uint32, len(info.Connections))
	for i, connection := range info.Connections {
		connIDs[i] = connection.Conn
	}
	sort.Slice(connIDs, func(i, j int) bool {
		return connIDs[i] < connIDs[j]
	})

	types := [][]string{}
	knownTypes := make(map[string]bool)
	for _, connID := range connIDs {
		connection := info.Connections[connID]
		if t := connection.Data.Type; !knownTypes[t] {
			types = append(types, []string{connection.Data.Type, fmt.Sprintf("[%s]", connection.Data.MD5Sum)})
			knownTypes[t] = true
		}
	}
	typesTable := &bytes.Buffer{}
	formatTable(typesTable, types, tablewriter.ALIGN_LEFT)

	topics := [][]string{}
	messageCounts := info.ConnectionMessageCounts()

	maxwidth := 0
	for _, connID := range connIDs {
		connection := info.Connections[connID]
		if width := digits(messageCounts[connection.Conn]); width > maxwidth {
			maxwidth = width
		}
	}

	for _, connID := range connIDs {
		connection := info.Connections[connID]
		topics = append(topics, []string{connection.Topic, fmt.Sprintf(
			"%*d msgs    : %s", maxwidth, messageCounts[connection.Conn], connection.Data.Type)})
	}
	topicsTable := &bytes.Buffer{}
	formatTable(topicsTable, topics, tablewriter.ALIGN_LEFT)

	tables := [][]string{
		{"types:", strings.TrimRight(typesTable.String(), "\n")},
		{"topics:", strings.TrimRight(topicsTable.String(), "\n")},
	}
	formatTable(w, tables, tablewriter.ALIGN_LEFT)

	if displayCaveats {
		fmt.Fprintf(w, "* estimated\n")
	}
}

var infoCmd = &cobra.Command{
	Use:   "info",
	Short: "Print summary information about a bag file",
	Long:  "Print summary information about a bag file. Compression statistics are estimates.",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			dief("info requires exactly one argument")
		}
		filename := args[0]
		f, err := os.Open(filename)
		if err != nil {
			dief("failed to open %s: %v", filename, err)
		}
		reader, err := rosbag.NewReader(f)
		if err != nil {
			dief("failed to construct reader: %s", err)
		}
		info, err := reader.Info()
		if err != nil {
			dief("failed to read info: %v", err)
		}

		fi, err := f.Stat()
		if err != nil {
			dief("failed to stat %s: %v", filename, err)
		}

		printInfo(os.Stdout, f, filename, fi.Size(), displayCaveats, info)
	},
}

func init() {
	rootCmd.AddCommand(infoCmd)
	infoCmd.PersistentFlags().BoolVarP(&displayCaveats, "display-caveats", "", false, "mark estimated fields")
}
