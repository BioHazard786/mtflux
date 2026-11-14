package main

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"sync/atomic"
	"syscall"
	"time"
)

// benchBytes counts payload bytes downloaded in phases 1 and 2, so CPU cost
// can be reported per MiB.
var benchBytes atomic.Int64

// profiler covers the measured phases only (after login, key generation and
// pool warm-up, which would otherwise dominate a CPU profile).
type profiler struct {
	cpuFile *os.File
	memPath string

	t0    time.Time
	ru0   syscall.Rusage
	ms0   runtime.MemStats
	b0    int64
	ready bool
}

func startProfile(cpuPath, memPath string) *profiler {
	p := &profiler{memPath: memPath}
	if cpuPath != "" {
		f, err := os.Create(cpuPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cpuprofile: %v\n", err)
		} else if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Fprintf(os.Stderr, "cpuprofile: %v\n", err)
			f.Close()
		} else {
			p.cpuFile = f
		}
	}
	p.t0 = time.Now()
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &p.ru0)
	runtime.ReadMemStats(&p.ms0)
	p.b0 = benchBytes.Load()
	p.ready = true
	return p
}

func (p *profiler) stop() {
	if p == nil || !p.ready {
		return
	}
	wall := time.Since(p.t0)
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	if p.cpuFile != nil {
		pprof.StopCPUProfile()
		p.cpuFile.Close()
	}

	tv := func(t syscall.Timeval) time.Duration {
		return time.Duration(t.Sec)*time.Second + time.Duration(t.Usec)*time.Microsecond
	}
	user := tv(ru.Utime) - tv(p.ru0.Utime)
	sys := tv(ru.Stime) - tv(p.ru0.Stime)
	cpu := user + sys
	mib := float64(benchBytes.Load()-p.b0) / (1 << 20)
	allocMiB := float64(ms.TotalAlloc-p.ms0.TotalAlloc) / (1 << 20)
	gcs := ms.NumGC - p.ms0.NumGC
	pause := time.Duration(ms.PauseTotalNs - p.ms0.PauseTotalNs)

	fmt.Println()
	fmt.Println("cpu usage (measured phases only)")
	fmt.Printf("  wall %v | cpu %v (user %v, sys %v) | avg %.2f cores | GOMAXPROCS %d\n",
		wall.Round(time.Millisecond), cpu.Round(time.Millisecond),
		user.Round(time.Millisecond), sys.Round(time.Millisecond),
		cpu.Seconds()/wall.Seconds(), runtime.GOMAXPROCS(0))
	if mib > 0 {
		fmt.Printf("  downloaded %.0f MiB | %.1f ms cpu per MiB | one core sustains ~%.0f MB/s at this cost\n",
			mib, cpu.Seconds()*1000/mib, mib/cpu.Seconds())
		fmt.Printf("  allocated %.0f MiB (%.1f bytes per payload byte) | %d GCs, %v total pause\n",
			allocMiB, allocMiB/mib, gcs, pause.Round(time.Microsecond))
	}

	if p.cpuFile != nil {
		fmt.Printf("  cpu profile: %s\n", p.cpuFile.Name())
	}
	if p.memPath != "" {
		f, err := os.Create(p.memPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "memprofile: %v\n", err)
			return
		}
		defer f.Close()
		if err := pprof.Lookup("allocs").WriteTo(f, 0); err != nil {
			fmt.Fprintf(os.Stderr, "memprofile: %v\n", err)
			return
		}
		fmt.Printf("  alloc profile: %s\n", p.memPath)
	}
}
