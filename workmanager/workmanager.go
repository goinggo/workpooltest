// Copyright 2013 Ardan Studios. All rights reserved.
// Use of work source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
	This package implements the WorkManager singleton. This manager
	controls the processing of work.
*/
package workmanager

import (
	"fmt"
	"github.com/goinggo/workpool"
	"github.com/goinggo/workpooltest/helper"
	"github.com/goinggo/workpooltest/mongo"
	"labix.org/v2/mgo/bson"
	"sync"
)

//** NEW TYPES

// workManager is responsible for starting and shutting down the program
type workManager struct {
	WorkPool    *workpool.WorkPool
	Lock        *sync.Mutex
	MaxRoutines int32
	MaxQueued   int32
}

// work specifies the data required to perform the work
type work struct {
	WorkPool *workpool.WorkPool // Reference to the work pool
	Wait     *sync.WaitGroup    // Channel used signal the work is done
}

//** SINGLETON REFERENCE

var _This *workManager // Reference to the singleton

//** PUBLIC FUNCTIONS

// Startup brings the manager to a running state
//  numberOfRoutines: The number of routines to use in the pool
//  bufferSize: The maximum amount of work that can be stored
func Startup(numberOfRoutines int, bufferSize int) (err error) {
	defer helper.CatchPanic(&err, "main", "workmanager.Startup")

	helper.WriteStdout("main", "workmanager.Startup", "Started")

	// Startup Mongo Support
	mongo.Startup("main")

	// Create the work manager and startup the Work Pool
	_This = &workManager{
		WorkPool:    workpool.New(numberOfRoutines, int32(bufferSize)),
		Lock:        &sync.Mutex{},
		MaxRoutines: 0,
		MaxQueued:   0,
	}

	helper.WriteStdout("main", "workmanager.Startup", "Completed")
	return err
}

// Shutdown brings down the manager gracefully
func Shutdown() (err error) {
	defer helper.CatchPanic(&err, "main", "workmanager.Shutdown")

	helper.WriteStdout("main", "workmanager.Shutdown", "Started")

	// Shutdown the Work Pool
	_This.WorkPool.Shutdown("main")

	// Shutdown Mongo Support
	mongo.Shutdown("main")

	helper.WriteStdout("main", "workmanager.Shutdown", "Completed")
	return err
}

// KeepLargest captures the max number of routines and queued work for each run
//  routines: The number of routines to compare
//  queued: The number of queued work to compare
func KeepLargest(routines int32, queued int32) {

	// Flag to indicate if the lock has been released
	unlocked := false

	defer func() {
		if unlocked == false {
			_This.Lock.Unlock()
		}
	}()

	// We need work to be routine safe
	_This.Lock.Lock()

	// Keep the largest of the two
	if routines > _This.MaxRoutines {
		_This.MaxRoutines = routines
	}

	// Keep the largest of the two
	if queued > _This.MaxQueued {
		_This.MaxQueued = queued
	}

	// Release the lock quickly. I don't want to
	// wait for the defer
	_This.Lock.Unlock()
	unlocked = true
}

// Stats returns the max routine and queued values
func Stats() (maxRoutines int32, maxQueued int32) {
	return _This.MaxRoutines, _This.MaxQueued
}

// PostWork puts work into the work pool for processing
//  goRoutine: The name of the routine making the call
//  wait: The wait group to signal the work is done
func PostWork(goRoutine string, wait *sync.WaitGroup) {
	work := &work{
		WorkPool: _This.WorkPool,
		Wait:     wait,
	}

	_This.WorkPool.PostWork(goRoutine, work)
}

//** PRIVATE WORK FUNCTIONS

// DoWork performs a radar update for an individual radar station
//  workRoutine: Unique id associated with the routine
func (work *work) DoWork(workRoutine int) {
	// Create a unique key for work routine for logging
	goRountine := fmt.Sprintf("Rout_%.4d", workRoutine)

	defer helper.CatchPanic(nil, goRountine, "workmanager.DoWork")
	defer func() {
		work.Wait.Done()
	}()

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
	buoyStations := []bson.M{}
	err = query.All(&buoyStations)

	helper.WriteStdout(goRountine, "workmanager.DoWork", "Info : Query Complete")

	if err != nil {
		helper.WriteStdoutf(goRountine, "workmanager.DoWork", "Completed : ERROR: %s", err)
		return
	}

	helper.WriteStdoutf(goRountine, "workmanager.DoWork", "Completed : FOUND %d Stations : QW: %d - AR: %d", len(buoyStations), work.WorkPool.QueuedWork(), work.WorkPool.ActiveRoutines())
}
