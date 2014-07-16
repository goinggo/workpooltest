// Copyright 2013 Ardan Studios. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
This program provides an sample to learn how a work pool can increase
performance and get more work done with less resources

Ardan Studios
12973 SW 112 ST, Suite 153
Miami, FL 33186
bill@ardanstudios.com

http://www.goinggo.net/2013/09/manage-work-using-work-pools.html
*/
package main

import (
	"fmt"
	"github.com/goinggo/workpooltest/helper"
	"github.com/goinggo/workpooltest/workmanager"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// main is the starting point of the program.
func main() {
	// Just verify we have the right number of arguments.
	if len(os.Args) != 3 {
		fmt.Printf("workpooltest routines logging\nex. workpooltest 24 off\n")
	}

	// If the word on is in the second argument, turn on logging.
	if strings.Contains(os.Args[2], "on") != true {
		helper.TurnLoggingOff()
	}

	// Give the runtime all the cores to use.
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Conver the number of routines for this run to an integer.
	numberOfRoutines, _ := strconv.Atoi(os.Args[1])

	// The number of database calls to make.
	amountOfWork := 100

	// Start the work manager so we can send in work.
	workmanager.Startup(numberOfRoutines, amountOfWork)

	// Capture all the duration for all the runs.
	var durations []time.Duration

	// Run the work five times to get an average.
	for runs := 0; runs < 5; runs++ {
		// Used to identify when all the work is completed.
		var waitGroup sync.WaitGroup

		// Mark the starting time.
		startTime := time.Now()

		// Post the 100 database calls and get this program going.
		waitGroup.Add(amountOfWork)
		for count := 0; count < amountOfWork; count++ {
			workmanager.PostWork("main", &waitGroup)
		}

		// Wait for all the work to be completed
		waitGroup.Wait()

		// Mark the ending time
		endTime := time.Now()

		// Caluclate the amount of time it took
		duration := endTime.Sub(startTime)

		// Capture the routine and queued stats
		maxRoutines, maxQueued := workmanager.Stats()

		// Display the run stats
		fmt.Printf("CPU[%d] Routines[%d] AmountOfWork[%d] Duration[%f] MaxRoutines[%d] MaxQueued[%d]\n", runtime.NumCPU(), numberOfRoutines, amountOfWork, duration.Seconds(), maxRoutines, maxQueued)

		// Capture the duration of the run
		durations = append(durations, duration)
	}

	// Calculate the total duration for all 5 runs
	var totalDuration float64
	for _, duration := range durations {
		totalDuration = totalDuration + duration.Seconds()
	}

	// Calculate the average of the 5 runs
	avgDuration := totalDuration / float64(len(durations))

	// Display the average
	fmt.Printf("Average[%f]\n", avgDuration)
}
