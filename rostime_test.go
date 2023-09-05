package rosbag

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestROSTimeRoundtrip(t *testing.T) {
	cases := []struct {
		assertion string
		input     uint64
	}{
		{
			"nanoseconds",
			100,
		},
		{
			"seconds",
			100e9 + 1000,
		},
		{
			"nanos",
			1693705645329000000,
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.assertion, func(t *testing.T) {
			rosTime := toRostime(c.input)
			normalTime := fromRostime(rosTime)
			assert.Equal(t, c.input, normalTime)
			buf := make([]byte, 8)
			putU64(buf, uint64(rosTime))
			assert.Equal(t, c.input, parseROSTime(buf))
		})
	}
}

func TestROSTimeString(t *testing.T) {
	cases := []struct {
		assertion string
		input     uint64
		output    string
	}{
		{
			"nanoseconds",
			100,
			"0.000000100",
		},
		{
			"seconds",
			100e9 + 1000,
			"100.000001000",
		},
		{
			"nanos",
			1693705645329000000,
			"1693705645.329000000",
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.assertion, func(t *testing.T) {
			rosTime := toRostime(c.input)
			assert.Equal(t, c.output, rosTime.String())
		})
	}
}
