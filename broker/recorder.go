package broker

import (
	"github.com/launchdarkly/foundation/ftime"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
)

type JobEntry struct {
	Status    string           `bson:"status,omitempty"`
	ExitCode  *int             `bson:"exitCode,omitempty"`
	Error     string           `bson:"error,omitempty"`
	Stdout    []byte           `bson:"stdout,omitempty"`
	Timestamp ftime.UnixMillis `bson:"timestamp"`
}

type JobRecord struct {
	Owner   string     `bson:"owner,omitempty"`
	JobId   uint64     `bson:"jobId"`
	Entries []JobEntry `bson:"entries"`
}

const (
	pending  = "pending"
	executed = "executed"
	buried   = "buried"
	timedOut = "timedout"
)

type Recorder struct {
	coll *mgo.Collection
}

func NewRecorder(url string) (recorder *Recorder, err error) {
	var session *mgo.Session
	session, err = mgo.Dial(url)

	if err != nil {
		return nil, err
	} else {
		db := session.DB("gonfalon")
		coll := jobs(db)
		createIndices(coll)

		return &Recorder{coll}, nil
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

func (r *Recorder) CreateRecord(owner string, id uint64) (err error) {
	entry := JobEntry{
		Status:    pending,
		Timestamp: ftime.Now(),
	}

	_, err = r.coll.Upsert(bson.M{"jobId": id}, bson.M{"$set": bson.M{"jobId": id, "owner": owner}, "$push": bson.M{"$each": []JobEntry{entry}, "$position": 0}})

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

func (r *Recorder) UpdateRecord(result JobResult) (err error) {
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

	_, err = r.coll.Upsert(bson.M{"jobId": result.JobId}, bson.M{"$set": bson.M{"jobId": result.JobId}, "$push": bson.M{"entries": entry}})

	return
}
