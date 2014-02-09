package workout

import (
	"errors"
	"sync/atomic"
	"time"
)

type Worker struct {
	client *Client
	master *Master
	id     int
}

var (
	ErrJobTimeout = errors.New("job timed out")
)

func NewWorker(m *Master, wid int) (w *Worker, err error) {
	//var err error

	w = new(Worker)
	w.master = m
	w.id = wid
	w.client, err = NewClient(m.url, m.tubes)

	if err != nil {
		return
	}

	if m.ReserveTimeout > time.Duration(0) {
		w.client.ReserveTimeout = m.ReserveTimeout
	}

	/*if err != nil {
		log.Warn("worker %d: client error: %s", wid, err)
	}*/

	return
}

func (w *Worker) run() {
	w.master.wg.Add(1)
	defer w.master.wg.Done()

	var job *Job
	var ok bool
	var err error

	logger.Infof("worker %d: starting", w.id)
	defer logger.Infof("worker %d: stopped", w.id)

	for {
		select {
		case <-w.master.quit:
			logger.Infof("worker %d: quitting...", w.id)
			return
		default:
		}

		if job, ok, err = w.client.Reserve(); !ok {
			continue
		}

		logger.Debugf("worker %d: got job %d", w.id, job.Id)

		atomic.AddInt32(&w.master.stat_active, 1)
		atomic.AddUint64(&w.master.stat_attempt, 1)
		err = w.process(job)

		if err != nil && w.master.stat_failure < w.master.max_retry {
			atomic.AddUint64(&w.master.stat_failure, 1)
			w.client.Release(job, err)
		} else {
			atomic.AddUint64(&w.master.stat_success, 1)
			w.client.Delete(job)
		}
		atomic.AddInt32(&w.master.stat_active, -1)

	}
}

func (w *Worker) process(job *Job) (err error) {
	t0 := time.Now()

	hfn, ok := w.master.handlers[job.Tube]
	if !ok {
		return Error("no handler registered")
	}

	to, ok := w.master.timeouts[job.Tube]
	if !ok {
		to = time.Duration(12) * time.Hour
	}

	ch := make(chan error)

	go func(fn JobHandler, j *Job) {
		ch <- fn(j)
	}(hfn, job)

	select {
	case err = <-ch:
	case <-time.After(to):
		err = ErrJobTimeout
	}

	dur := time.Now().Sub(t0)

	cfn, ok := w.master.callbacks[job.Tube]
	if ok && cfn != nil {
		cfn(job, err, dur)
	}

	return
}
