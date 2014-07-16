// Copyright 2013 Ardan Studios. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package mongo provides mongo connectivity support.
package mongo

import (
	"github.com/goinggo/workpooltest/helper"
	"labix.org/v2/mgo"
	"time"
)

const (
	mongoHost     = "ds035428.mongolab.com:35428"
	mongoDatabase = "goinggo"
	mongoUsername = "guest"
	mongoPassword = "welcome"
)

// mongoManager manages a connection and session.
type mongoManager struct {
	MongoDBDialInfo *mgo.DialInfo // The connection information.
	MongoSession    *mgo.Session
}

var mm mongoManager // Reference to the mm.

//** PUBLIC FUNCTIONS

// Startup brings the manager to a running state.
func Startup(goRoutine string) error {
	var err error
	defer helper.CatchPanic(&err, goRoutine, "Startup")
	helper.WriteStdout(goRoutine, "mongo.Startup", "Started")

	// Create the mm
	mm = mongoManager{
		MongoDBDialInfo: &mgo.DialInfo{
			Addrs:    []string{mongoHost},
			Timeout:  10 * time.Second,
			Database: mongoDatabase,
			Username: mongoUsername,
			Password: mongoPassword,
		},
	}

	// Capture a master session for use
	mm.MongoSession, err = GetSession(goRoutine)

	helper.WriteStdout(goRoutine, "mongo.Startup", "Completed")
	return err
}

// Shutdown systematically brings the manager down gracefully.
func Shutdown(goRoutine string) error {
	var err error
	defer helper.CatchPanic(&err, goRoutine, "Shutdown")
	helper.WriteStdout(goRoutine, "mongo.Shutdown", "Started")

	// Close the master session
	CloseSession(goRoutine, mm.MongoSession)

	helper.WriteStdout(goRoutine, "mongo.Shutdown", "Completed")
	return err
}

// GetSession grabs a connection from the pool for use.
func GetSession(goRoutine string) (*mgo.Session, error) {
	var err error
	defer helper.CatchPanic(&err, goRoutine, "GetSession")
	helper.WriteStdout(goRoutine, "mongo.GetSession", "Started")

	// Establish a session MongoDB
	mongoSession, err := mgo.DialWithInfo(mm.MongoDBDialInfo)
	if err != nil {
		helper.WriteStdoutf(goRoutine, "mongo.GetSession", "ERROR : %s", err)
		return nil, err
	}

	// Reads and writes will always be made to the master server using a
	// unique connection so that reads and writes are fully consistent,
	// ordered, and observing the most up-to-date data.
	mongoSession.SetMode(mgo.Strong, true)

	// Have the session check for errors.
	mongoSession.SetSafe(&mgo.Safe{})

	// Don't want any longer than 10 second for an operation to complete.
	mongoSession.SetSyncTimeout(10 * time.Second)

	helper.WriteStdout(goRoutine, "mongo.GetSession", "Completed")
	return mongoSession, err
}

// CopySession get a new connection based on an existing connection.
func CopySession(goRoutine string) (*mgo.Session, error) {
	var err error
	defer helper.CatchPanic(&err, goRoutine, "GetSession")
	helper.WriteStdout(goRoutine, "mongo.GetSession", "Started")

	// Copy the master session
	mongoSession := mm.MongoSession.Copy()

	helper.WriteStdout(goRoutine, "mongo.GetSession", "Completed")
	return mongoSession, err
}

// CloseSession puts the connection back into the pool.
func CloseSession(goRoutine string, mongoSession *mgo.Session) {
	defer helper.CatchPanic(nil, goRoutine, "CloseSession")
	helper.WriteStdout(goRoutine, "mongo.CloseSession", "Started")

	// Close the specified session
	mongoSession.Close()

	helper.WriteStdout(goRoutine, "mongo.CloseSession", "Completed")
}

// GetCollection returns a reference to a collection for the specified database and collection name.
func GetCollection(mongoSession *mgo.Session, collectionName string) (collection *mgo.Collection) {
	return mongoSession.DB(mongoDatabase).C(collectionName)
}
