package cmd

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/foxglove/go-rosbag/rosbag"
	"github.com/foxglove/go-rosbag/rosbag/ros1msg"
	"github.com/spf13/cobra"
)

var (
	linear bool
	simple bool
)

// readIndexed reads the file in index order.
func processMessages(f io.ReadSeeker, linear bool, callbacks ...func(*rosbag.Connection, *rosbag.Message) error) error {
	reader, err := rosbag.NewReader(f)
	if err != nil {
		return fmt.Errorf("failed to construct reader: %w", err)
	}
	it, err := reader.Messages(rosbag.ScanLinear(linear))
	if err != nil {
		return fmt.Errorf("failed to create message iterator: %w", err)
	}
	for it.More() {
		conn, msg, err := it.Next()
		if err != nil {
			return fmt.Errorf("failed to read message: %w", err)
		}
		for _, callback := range callbacks {
			err = callback(conn, msg)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func writeMessage(
	w io.Writer,
	topic string,
	time uint64,
	data []byte,
) error {
	_, err := w.Write([]byte(`{"topic": "`))
	if err != nil {
		return err
	}
	_, err = w.Write([]byte(topic))
	if err != nil {
		return err
	}
	_, err = w.Write([]byte(`", "time": `))
	if err != nil {
		return err
	}
	_, err = w.Write([]byte(fmt.Sprintf("%d", time)))
	if err != nil {
		return err
	}
	_, err = w.Write([]byte(`, "data": `))
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	if err != nil {
		return err
	}
	_, err = w.Write([]byte("}\n"))
	if err != nil {
		return err
	}
	return nil
}

func simpleMessageHandler(conn *rosbag.Connection, msg *rosbag.Message) error {
	displayLen := min(10, len(msg.Data))
	fmt.Printf("%d %s [%s] %v...\n", msg.Time, conn.Topic, conn.Data.Type, msg.Data[:displayLen])
	return nil
}

type messageHandler func(*rosbag.Connection, *rosbag.Message) error

func jsonMessageHandler(w io.Writer) messageHandler {
	transcoders := make(map[uint32]*ros1msg.JSONTranscoder)
	rosdata := &bytes.Reader{}
	jsondata := &bytes.Buffer{}
	var err error
	return func(conn *rosbag.Connection, msg *rosbag.Message) error {
		xcoder, ok := transcoders[conn.Conn]
		if !ok {
			packageName := strings.Split(conn.Data.Type, "/")[0]
			xcoder, err = ros1msg.NewJSONTranscoder(packageName, conn.Data.MessageDefinition)
			if err != nil {
				return err
			}
			transcoders[conn.Conn] = xcoder
		}
		rosdata.Reset(msg.Data)
		err := xcoder.Transcode(jsondata, rosdata)
		if err != nil {
			return err
		}
		err = writeMessage(
			w,
			conn.Topic,
			msg.Time,
			jsondata.Bytes(),
		)
		if err != nil {
			return err
		}
		jsondata.Reset()
		return nil
	}
}

var catCmd = &cobra.Command{
	Use:   "cat [file]",
	Short: "Extract messages from a bag file",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			dief("cat command requires exactly one argument")
		}
		filename := args[0]
		f, err := os.Open(filename)
		if err != nil {
			dief("failed to open %s: %v", filename, err)
		}

		var handler messageHandler
		if simple {
			handler = simpleMessageHandler
		} else {
			handler = jsonMessageHandler(os.Stdout)
		}

		err = processMessages(f, linear, handler)
		if err != nil {
			dief("failed to process messages: %s", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(catCmd)
	catCmd.PersistentFlags().BoolVarP(&linear, "linear", "", false, "read messages in linear order, without reading index")
	catCmd.PersistentFlags().BoolVarP(&simple, "simple", "", false, "print topics and timestamps instead of JSON data")
}
