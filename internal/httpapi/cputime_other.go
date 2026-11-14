//go:build !unix

package httpapi

import "time"

func processCPUTime() time.Duration { return 0 }
