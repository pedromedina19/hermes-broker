package monitoring

import (
	"log"
	"runtime"
	"time"
)

func StartMonitoring(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		for range ticker.C {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			bToMb := func(b uint64) uint64 {
				return b / 1024 / 1024
			}

			log.Printf("📊 [METRICS] Goroutines: %d | Alloc: %v MiB | TotalAlloc: %v MiB | Sys: %v MiB | NumGC: %v",
				runtime.NumGoroutine(),
				bToMb(m.Alloc),      // Memory in use now
				bToMb(m.TotalAlloc), // Total allocated since the beginning (cumulative)
				bToMb(m.Sys),        // Total time that the OS has reserved for the process.
				m.NumGC,             // Garbage Collection Cycles
			)
		}
	}()
}