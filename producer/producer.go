package producer

import (
	"encoding/json"
	"github.com/kr/beanstalk"
	r "github.com/launchdarkly/cmdstalk/recorder"
	"time"
)

type JobProducer struct {
	conn     *beanstalk.Conn
	recorder *r.JobRecorder
}

func New(address string, mongoUrl string) (*JobProducer, error) {
	conn, err := beanstalk.Dial("tcp", address)

	if err != nil {
		return nil, err
	}

	recorder, err := r.New(mongoUrl)

	if err != nil {
		return nil, err
	}

	return &JobProducer{conn, recorder}, nil
}

func (p *JobProducer) RegisterJob(cmd, owner string, pri uint32, delay, ttr time.Duration, jsonData interface{}) (id uint64, err error) {
	tube := beanstalk.Tube{p.conn, cmd}

	data, err := json.Marshal(jsonData)

	if err != nil {
		return 0, err
	}

	id, err = tube.Put(data, pri, delay, ttr)

	if err != nil {
		return 0, err
	}

	err = p.recorder.RecordJob(owner, id, cmd, jsonData)

	return
}
