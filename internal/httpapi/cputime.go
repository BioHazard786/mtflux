package httpapi

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// cpuPercent is CPU time as a percentage of wall time: 100 means one core was
// busy the whole time. Very short spans give no usable reading and return 0.
func cpuPercent(cpu, wall time.Duration) int {
	if wall < 200*time.Millisecond || cpu <= 0 {
		return 0
	}
	return int(100*cpu.Seconds()/wall.Seconds() + 0.5)
}

// hostCPU is the machine-wide CPU counters from /proc/stat, in clock ticks.
type hostCPU struct {
	total, idle uint64
	ok          bool
}

// readHostCPU reads the aggregate "cpu" line of /proc/stat. Inside a
// container that file still describes the whole machine, and unlike the
// process's own CPU time it includes network interrupt work done by the
// kernel on the server's behalf. Not available outside Linux.
func readHostCPU() hostCPU {
	b, err := os.ReadFile("/proc/stat")
	if err != nil {
		return hostCPU{}
	}
	line, _, _ := strings.Cut(string(b), "\n")
	return parseHostCPU(line)
}

func parseHostCPU(line string) hostCPU {
	f := strings.Fields(line)
	if len(f) < 5 || f[0] != "cpu" {
		return hostCPU{}
	}
	var c hostCPU
	for i, v := range f[1:] {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return hostCPU{}
		}
		if i >= 8 {
			break // guest time is already part of user time
		}
		c.total += n
		if i == 3 || i == 4 { // idle, iowait
			c.idle += n
		}
	}
	c.ok = true
	return c
}

// hostBusyPercent is the share of the machine's CPU that was busy between two
// readings: 100 means every core was busy. It returns 0 without a reading.
func hostBusyPercent(a, b hostCPU) int {
	if !a.ok || !b.ok || b.total <= a.total {
		return 0
	}
	total, idle := b.total-a.total, b.idle-a.idle
	if idle > total {
		return 0
	}
	return int(100*float64(total-idle)/float64(total) + 0.5)
}
