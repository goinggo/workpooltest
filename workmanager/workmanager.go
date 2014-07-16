// Copyright 2013 Ardan Studios. All rights reserved.
// Use of work source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package workmanager implements the WorkManager singleton. This manager
// controls the processing of work.
package workmanager

import (
	"fmt"
	"github.com/goinggo/workpool"
	"github.com/goinggo/workpooltest/helper"
	"github.com/goinggo/workpooltest/mongo"
	"labix.org/v2/mgo/bson"
	"sync"
)

// workManager is responsible for starting and shutting down the program.
type workManager struct {
	WorkPool    *workpool.WorkPool
	Lock        *sync.Mutex
	MaxRoutines int32
	MaxQueued   int32
}

// work specifies the data required to perform the work.
type work struct {
	WorkPool *workpool.WorkPool // Reference to the work pool.
	Wait     *sync.WaitGroup    // Channel used signal the work is done.
}

var wm workManager // Reference to the singleton

//** PUBLIC FUNCTIONS

// Startup brings the manager to a running state.
func Startup(numberOfRoutines int, bufferSize int) error {
	var err error
	defer helper.CatchPanic(&err, "main", "workmanager.Startup")
	helper.WriteStdout("main", "workmanager.Startup", "Started")

	// Startup Mongo Support
	mongo.Startup("main")

	// Create the work manager and startup the Work Pool
	wm = workManager{
		WorkPool:    workpool.New(numberOfRoutines, int32(bufferSize)),
		Lock:        &sync.Mutex{},
		MaxRoutines: 0,
		MaxQueued:   0,
	}

	helper.WriteStdout("main", "workmanager.Startup", "Completed")
	return err
}

// Shutdown brings down the manager gracefully
func Shutdown() error {
	var err error
	defer helper.CatchPanic(&err, "main", "workmanager.Shutdown")
	helper.WriteStdout("main", "workmanager.Shutdown", "Started")

	// Shutdown the Work Pool
	wm.WorkPool.Shutdown("main")

	// Shutdown Mongo Support
	mongo.Shutdown("main")

	helper.WriteStdout("main", "workmanager.Shutdown", "Completed")
	return err
}

// KeepLargest captures the max number of routines and queued work for each run
//  routines: The number of routines to compare
//  queued: The number of queued work to compare
func KeepLargest(routines int32, queued int32) {
	// We need work to be routine safe.
	wm.Lock.Lock()
	defer wm.Lock.Unlock()

	// Keep the largest of the two
	if routines > wm.MaxRoutines {
		wm.MaxRoutines = routines
	}

	// Keep the largest of the two
	if queued > wm.MaxQueued {
		wm.MaxQueued = queued
	}
}

// Stats returns the max routine and queued values.
func Stats() (maxRoutines int32, maxQueued int32) {
	return wm.MaxRoutines, wm.MaxQueued
}

// PostWork puts work into the work pool for processing.
func PostWork(goRoutine string, wait *sync.WaitGroup) {
	work := work{
		WorkPool: wm.WorkPool,
		Wait:     wait,
	}

	wm.WorkPool.PostWork(goRoutine, &work)
}

// DoWork performs a radar update for an individual radar station.
func (work *work) DoWork(workRoutine int) {
	// Create a unique key for work routine for logging
	goRountine := fmt.Sprintf("Rout_%.4d", workRoutine)

	defer helper.CatchPanic(nil, goRountine, "workmanager.DoWork")
	defer work.Wait.Done()

	// Take a snapshot of the work pool stats and keep the largest
	KeepLargest(work.WorkPool.ActiveRoutines(), work.WorkPool.QueuedWork())

	helper.WriteStdoutf(goRountine, "workmanager.DoWork", "Started : QW: %d - AR: %d", work.WorkPool.QueuedWork(), work.WorkPool.ActiveRoutines())

	// Grab a mongo session
	mongoSession, err := mongo.CopySession(goRountine)
	if err != nil {
		helper.WriteStdoutf(goRountine, "workmanager.DoWork", "Completed : ERROR: %s", err)
		return
	}

	// Close the session when the work is complete
	defer mongo.CloseSession(goRountine, mongoSession)

	// Access the buoy_stations collection
	collection := mongo.GetCollection(mongoSession, "buoy_stations")

	// Find all the buoys
	query := collection.Find(nil).Sort("station_id")

	helper.WriteStdout(goRountine, "workmanager.DoWork", "Info : Performing Query")

	// Capture all of the buoys
	var buoyStations []bson.M
	if err = query.All(&buoyStations); err != nil {
		helper.WriteStdoutf(goRountine, "workmanager.DoWork", "Completed : ERROR: %s", err)
		return
	}

	helper.WriteStdoutf(goRountine, "workmanager.DoWork", "Completed : FOUND %d Stations : QW: %d - AR: %d", len(buoyStations), work.WorkPool.QueuedWork(), work.WorkPool.ActiveRoutines())
}
