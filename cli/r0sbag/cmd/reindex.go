package cmd

import (
	"fmt"
	"io"
	"os"

	"github.com/foxglove/go-rosbag/rosbag"
	"github.com/spf13/cobra"
)

var (
	compression string
	force       bool
)

// rewrite messages from reader to writer. Does not close writer when done, to
// finalize indexes - that is the responsibility of the caller.
func rewrite(w *rosbag.Writer, r io.Reader) error {
	connections := make(map[uint32]bool)
	return processMessages(r, true, func(conn *rosbag.Connection, msg *rosbag.Message) error {
		if !connections[conn.Conn] {
			if err := w.WriteConnection(conn); err != nil {
				return fmt.Errorf("failed to write message: %w", err)
			}
			connections[conn.Conn] = true
		}
		if err := w.WriteMessage(msg); err != nil {
			return fmt.Errorf("failed to write message: %w", err)
		}
		return nil
	})
}

func fileIsIndexed(rs io.ReadSeeker) bool {
	currentPos, err := rs.Seek(0, io.SeekCurrent)
	if err != nil {
		return false
	}
	defer func() {
		_, err = rs.Seek(currentPos, io.SeekStart)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to seek back to original position: %s\n", err)
		}
	}()
	reader, err := rosbag.NewReader(rs)
	if err != nil {
		return false
	}
	_, err = reader.Info()
	return err == nil
}

var reindexCmd = &cobra.Command{
	Use:   "reindex [file]",
	Short: "Reindex a bag file and physically sort the messages",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			dief("reindex requires exactly one argument")
		}
		filename := args[0]
		f, err := os.Open(filename)
		if err != nil {
			dief("failed to open file: %s", err)
		}
		defer f.Close()

		// if the file is already indexed
		if !force && fileIsIndexed(f) {
			fmt.Fprintf(os.Stderr, "%s is already indexed\n", filename)
			return
		}

		tmpfile, err := os.CreateTemp("", fmt.Sprintf("%s.reindex.temp", filename))
		if err != nil {
			dief("failed to create temp file: %s", err)
		}

		writer, err := rosbag.NewWriter(tmpfile, rosbag.WithCompression(compression))
		if err != nil {
			dief("failed to create writer: %s", err)
		}

		err = rewrite(writer, f)
		if err != nil {
			// We tolerate most errors here, since the input is
			// expected to be corrupt. The bytes we wrote out are
			// presumed good.
			fmt.Fprintf(os.Stderr, "Detected error in input: %s\n", err)
		}

		err = writer.Close()
		if err != nil {
			dief("failed to close writer: %s", err)
		}

		err = tmpfile.Close()
		if err != nil {
			dief("failed to close temp file: %s", err)
		}

		err = f.Close()
		if err != nil {
			dief("failed to close original file: %s", err)
		}

		// move some files around. The original input is going to go to
		// "path.orig". The tempfile will go to the original input
		// location.
		err = os.Rename(filename, filename+".orig")
		if err != nil {
			dief("failed to rename original file: %s", err)
		}
		err = os.Rename(tmpfile.Name(), filename)
		if err != nil {
			dief("failed to rename temp file: %s", err)
		}
		fmt.Fprintf(os.Stderr, "%s reindexed. Original file moved to %s.orig\n", filename, filename)
	},
}

func init() {
	rootCmd.AddCommand(reindexCmd)
	reindexCmd.PersistentFlags().StringVarP(&compression, "compression", "", "lz4", "output compression algorithm")
	reindexCmd.PersistentFlags().BoolVarP(&force, "force", "f", false, "force reindexing of already-indexed bags")
}
