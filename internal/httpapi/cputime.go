package httpapi

import "time"

// cpuPercent is CPU time as a percentage of wall time: 100 means one core was
// busy the whole time. Very short spans give no usable reading and return 0.
func cpuPercent(cpu, wall time.Duration) int {
	if wall < 200*time.Millisecond || cpu <= 0 {
		return 0
	}
	return int(100*cpu.Seconds()/wall.Seconds() + 0.5)
}
