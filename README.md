## go-rosbag

Utilities for working with ROS bag data in go.

### rosbag library
The rosbag library contains utilities for reading and writing bag files. The
primary focus is on the bag container format, rather than the ROS message
serialization format - although there is a ros1msg parser.

#### Writing a bag file

Write a bag file like this:

```go

f, err := os.Open("my-bag.bag")
if err != nil {
	panic(err)
}

writer, err := rosbag.NewWriter(f)
if err != nil {
	panic(err)
}

err = writer.WriteConnection(&rosbag.Connection{
	Conn: 1,
	Topic: "/foo",
	Data: rosbag.ConnectionHeader{
		Topic: "/foo",
		Type: "Foo",
		MD5Sum: "abc",
		MessageDefinition: []byte{0x01, 0x02, 0x03},

		// optional fields
		CallerID: &callerID,
		Latching: &latching,
	},
})
if err != nil {
	panic(err)
}

for i := 0; i < 100; i++ {
	err = writer.WriteMessage(&rosbag.Message{
		Conn: 0,
		Time: uint64(i),
		Data: []byte{"hello"},
	})
	if err != nil {
		panic(err)
	}
}

```


#### Reading a bag file

Read a bag file like this:

```go

f, err := os.Open("my-bag.bag")
if err != nil {
	panic(err)
}

reader, err := rosbag.NewReader(f)
if err != nil {
	panic(err)
}

it, err := reader.Messages()
if err != nil {
	panic(err)
}

for it.More() {
	connection, message, err := it.Next()
	if err != nil {
		panic(err)
	}

	// handle connection and message
}

```
