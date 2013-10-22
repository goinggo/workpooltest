// Copyright 2013 Ardan Studios. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
	This package provides mongo connectivity support
*/
package mongo

import (
	"github.com/goinggo/workpooltest/helper"
	"labix.org/v2/mgo"
	"time"
)

//** CONSTANTS

const (
	MONGODB_HOST     = "ds035428.mongolab.com:35428"
	MONGODB_DATABASE = "goinggo"
	MONGODB_USERNAME = "guest"
	MONGODB_PASSWORD = "welcome"
)

//** NEW TYPES

// mongoManager manages a connection and session
type mongoManager struct {
	MongoDBDialInfo *mgo.DialInfo // The connection information
	MongoSession    *mgo.Session
}

//** SINGLETON REFERENCE

var _This *mongoManager // Reference to the singleton

//** PUBLIC FUNCTIONS

// Startup brings the manager to a running state
//  goRoutine: The name of the routine running the code
func Startup(goRoutine string) (err error) {
	defer helper.CatchPanic(&err, goRoutine, "Startup")

	helper.WriteStdout(goRoutine, "mongo.Startup", "Started")

	if err != nil {
		helper.WriteStdoutf(goRoutine, "mongo.Startup", "Completed : ERROR : %s", err)
		return err
	}

	// Create the singleton
	_This = &mongoManager{
		MongoDBDialInfo: &mgo.DialInfo{
			Addrs:    []string{MONGODB_HOST},
			Timeout:  10 * time.Second,
			Database: MONGODB_DATABASE,
			Username: MONGODB_USERNAME,
			Password: MONGODB_PASSWORD,
		},
	}

	// Capture a master session for use
	_This.MongoSession, err = GetSession(goRoutine)

	helper.WriteStdout(goRoutine, "mongo.Startup", "Completed")
	return err
}

// Shutdown systematically brings the manager down gracefully
//  goRoutine: The name of the routine running the code
func Shutdown(goRoutine string) (err error) {
	defer helper.CatchPanic(&err, goRoutine, "Shutdown")

	helper.WriteStdout(goRoutine, "mongo.Shutdown", "Started")

	// Close the master session
	CloseSession(goRoutine, _This.MongoSession)

	helper.WriteStdout(goRoutine, "mongo.Shutdown", "Completed")
	return err
}

// GetSession grabs a connection from the pool for use
//  goRoutine: The name of the routine running the code
//  mongoSession: The session to use to make the call
func GetSession(goRoutine string) (mongoSession *mgo.Session, err error) {
	defer helper.CatchPanic(&err, goRoutine, "GetSession")

	helper.WriteStdout(goRoutine, "mongo.GetSession", "Started")

	// Establish a session MongoDB
	mongoSession, err = mgo.DialWithInfo(_This.MongoDBDialInfo)

	if err != nil {
		helper.WriteStdoutf(goRoutine, "mongo.GetSession", "ERROR : %s", err)
		return nil, err
	}

	// Reads and writes will always be made to the master server using a
	// unique connection so that reads and writes are fully consistent,
	// ordered, and observing the most up-to-date data.
	mongoSession.SetMode(mgo.Strong, true)

	// Have the session check for errors
	mongoSession.SetSafe(&mgo.Safe{})

	// Don't want any longer than 10 second for an operation to complete
	mongoSession.SetSyncTimeout(10 * time.Second)

	helper.WriteStdout(goRoutine, "mongo.GetSession", "Completed")
	return mongoSession, err
}

// CopySession get a new connection based on an existing connection
//  goRoutine: The name of the routine running the code
func CopySession(goRoutine string) (mongoSession *mgo.Session, err error) {
	defer helper.CatchPanic(&err, goRoutine, "GetSession")

	helper.WriteStdout(goRoutine, "mongo.GetSession", "Started")

	// Copy the master session
	mongoSession = _This.MongoSession.Copy()

	helper.WriteStdout(goRoutine, "mongo.GetSession", "Completed")
	return mongoSession, err
}

// CloseSession puts the connection back into the pool
//  goRoutine: The name of the routine running the code
//  mongoSession: The session to use to make the call
func CloseSession(goRoutine string, mongoSession *mgo.Session) {
	defer helper.CatchPanic(nil, goRoutine, "CloseSession")

	helper.WriteStdout(goRoutine, "mongo.CloseSession", "Started")

	// Close the specified session
	mongoSession.Close()

	helper.WriteStdout(goRoutine, "mongo.CloseSession", "Completed")
}

// GetCollection returns a reference to a collection for the specified database and collection name
//  mongoSession: The session to use to make the call
//  collectionName: The name of the collection to access
func GetCollection(mongoSession *mgo.Session, collectionName string) (collection *mgo.Collection) {
	// Access the specified collection
	return mongoSession.DB(MONGODB_DATABASE).C(collectionName)
}
