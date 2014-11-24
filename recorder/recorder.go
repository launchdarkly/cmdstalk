package recorder

import (
	"github.com/launchdarkly/foundation/ftime"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
)

type JobResult struct {

	// Buried is true if the job was buried.
	Buried bool

	// Executed is true if the job command was executed (or attempted).
	Executed bool

	// ExitStatus of the command; 0 for success.
	ExitStatus int

	// JobId from beanstalkd.
	JobId uint64

	// Stdout of the command.
	Stdout []byte

	// TimedOut indicates the worker exceeded TTR for the job.
	// Note this is tracked by a timer, separately to beanstalkd.
	TimedOut bool

	// Error raised while attempting to handle the job.
	Error error
}

type JobEntry struct {
	Status    string           `bson:"status,omitempty"`
	ExitCode  *int             `bson:"exitCode,omitempty"`
	Error     string           `bson:"error,omitempty"`
	Stdout    []byte           `bson:"stdout,omitempty"`
	Timestamp ftime.UnixMillis `bson:"timestamp"`
}

type JobRecord struct {
	Owner   string      `bson:"owner,omitempty"`
	JobId   uint64      `bson:"jobId"`
	Cmd     string      `bson:"cmd"`
	Entries []JobEntry  `bson:"entries"`
	Data    interface{} `bson:"data"`
}

const (
	pending  = "pending"
	executed = "executed"
	buried   = "buried"
	timedOut = "timedout"
)

type JobRecorder struct {
	session *mgo.Session
}

func New(url string) (recorder *JobRecorder, err error) {
	var session *mgo.Session
	session, err = mgo.Dial(url)

	if err != nil {
		return nil, err
	} else {
		db := session.DB("gonfalon")
		coll := jobs(db)
		createIndices(coll)

		return &JobRecorder{session}, nil
	}
}

func jobs(db *mgo.Database) *mgo.Collection {
	return db.C("jobs")
}

func createIndices(coll *mgo.Collection) (err error) {
	err = coll.EnsureIndex(mgo.Index{
		Key:        []string{"jobId"},
		Unique:     true,
		DropDups:   false,
		Background: true,
		Sparse:     false,
	})

	if err != nil {
		return
	}

	err = coll.EnsureIndex(mgo.Index{
		Key:        []string{"owner, jobId"},
		Unique:     true,
		DropDups:   false,
		Background: true,
		Sparse:     true,
	})

	return
}

func (r *JobRecorder) RecordJob(owner string, id uint64, cmd string, data interface{}) (err error) {
	db := r.session.Clone().DB("gonfalon")
	defer db.Session.Close()

	entry := JobEntry{
		Status:    pending,
		Timestamp: ftime.Now(),
	}

	_, err = jobs(db).Upsert(bson.M{"jobId": id}, bson.M{"$set": bson.M{"jobId": id, "owner": owner, "data": data, "cmd": cmd}, "$push": bson.M{"entries": bson.M{"$each": []JobEntry{entry}, "$position": 0}}})

	return
}

func status(result JobResult) string {
	if result.Buried {
		return buried
	} else if result.Executed {
		return executed
	} else if result.TimedOut {
		return timedOut
	} else {
		return "unknown"
	}
}

func exitCode(result JobResult) *int {
	if result.Executed {
		return &result.ExitStatus
	} else {
		return nil
	}
}

func (r *JobRecorder) UpdateJob(result JobResult) (err error) {
	db := r.session.Clone().DB("gonfalon")
	defer db.Session.Close()

	var jobErr string
	if result.Error != nil {
		jobErr = result.Error.Error()
	}

	entry := JobEntry{
		Status:    status(result),
		ExitCode:  exitCode(result),
		Error:     jobErr,
		Stdout:    result.Stdout,
		Timestamp: ftime.Now(),
	}

	_, err = jobs(db).Upsert(bson.M{"jobId": result.JobId}, bson.M{"$set": bson.M{"jobId": result.JobId}, "$push": bson.M{"entries": entry}})

	return
}
