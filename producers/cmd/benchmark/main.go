package main

import (
	"fmt"
	"time"
	"tillstream/producers/internal/generator"
)

func main() {
	fmt.Println("🚀 Starting Golang Generator Throughput Benchmark...")
	fmt.Println("==================================================")
	
	gen := generator.NewGenerator(42)
	target := 1_000_000 // 1 Million records
	
	fmt.Printf("⏳ Generating %d simulated multi-tenant transactions in-memory...\n", target)
	
	start := time.Now()
	for i := 0; i < target; i++ {
		gen.GenerateOrderFlow()
	}
	elapsed := time.Since(start)
	
	tps := float64(target) / elapsed.Seconds()
	
	fmt.Printf("\n✅ Generation Complete in %s\n", elapsed)
	fmt.Printf("🏎️  Max Theoretical Throughput (CPU Bound): %.2f TPS\n", tps)
	fmt.Println("\n(Note: This isolates CPU data generation speed to prove the '100k TPS' claim, factoring out network I/O to Kafka)")
	fmt.Println("==================================================")
}
