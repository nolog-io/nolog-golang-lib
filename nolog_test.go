package nolog

import (
	"log"
	"runtime"
	"sync"
	"testing"
	"time"
)

var (
	block1     = CreateObjective("Block1")
	blockAlert = block1.WithAlert("Sample alert")
	dep1       = block1.AddDependency("dep1", "ac")
	dep2       = block1.AddDependency("dep2", "ac")
	dep3       = block1.AddDependency("dep3", "ac")
	dep4       = block1.AddDependency("dep4", "ac")
	dep5       = block1.AddDependency("dep5", "ac")
	dep6       = block1.AddDependency("dep6", "ac")
)

func TestPerformance(t *testing.T) {
	Initialize("1", "2", "3", "local")
	for _, size := range []int{10, 100, 1000, 10000, 100000} {
		log.Printf("RUNNING SIZE %d ---", size)
		run(size)
	}
	for _, size := range []int{10, 100, 1000, 10000, 100000} {
		log.Printf("P-RUNNING SIZE %d ---", size)
		prun(size)
	}
	log.Print("FIN")
	time.Sleep(15 * time.Second)
}

func work(i int) {
	executionBlock1 := block1.Start()
	depExec1 := executionBlock1.StartDependency(dep1)
	depExec6 := executionBlock1.StartDependency(dep6)
	depExec5 := executionBlock1.StartDependency(dep5)
	depExec1.Success()
	depExec2 := executionBlock1.StartDependency(dep2)
	depExec2.Success()
	depExec3 := executionBlock1.StartDependency(dep3)
	depExec3.Success()
	depExec4 := executionBlock1.StartDependency(dep4)
	depExec4.Success()
	depExec6.Success()
	depExec5.Success()
	if i%2 == 0 {
		executionBlock1.Success()
	} else {
		executionBlock1.Fail(blockAlert, "")
	}
}

func prun(r int) {
	pool := make(chan int, 10)
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		go func() {
			for v := range pool {
				work(v)
				wg.Done()
			}
		}()
	}
	wg.Add(r)
	start := time.Now()
	for i := 0; i < r; i++ {
		pool <- i
	}
	wg.Wait()
	end := time.Since(start)
	close(pool)
	log.Printf("--- %.20f seconds ---", end.Seconds())
	runtime.GC()
}

func run(r int) {
	start := time.Now()
	for i := 0; i < r; i++ {
		work(i)
	}
	end := time.Since(start)
	log.Printf("--- %.20f seconds ---", end.Seconds())
	runtime.GC()
}
