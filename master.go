package workout

import (
	"github.com/bububa/raven-go/raven"
	"sync"
	"sync/atomic"
	"time"
)

const SENTRY_DNS = "http://4b27200647234ab2a00c4452df8afe1c:a879047d88e1455dbf0ea055f3826c4b@sentry.xibao100.com/3"

type JobHandler func(*Job) error
type JobCallback func(*Job, error, time.Duration)

type Stats struct {
	stat_active  int32
	stat_attempt uint64
	stat_success uint64
	stat_failure uint64
}

type Master struct {
	ReserveTimeout time.Duration
	concurrency    int
	url            string
	tubes          []string
	workers        []*Worker
	callbacks      map[string]JobCallback
	handlers       map[string]JobHandler
	timeouts       map[string]time.Duration
	job            chan *Job
	quit           chan bool
	stat_active    int32
	stat_attempt   uint64
	stat_success   uint64
	stat_failure   uint64
	max_retry      uint64
	sentry         *raven.Client
	mg             sync.WaitGroup
	wg             sync.WaitGroup
}

func (m *Master) Stats() (s *Stats) {
	s = new(Stats)
	s.stat_active = atomic.LoadInt32(&m.stat_active)
	s.stat_attempt = atomic.LoadUint64(&m.stat_attempt)
	s.stat_success = atomic.LoadUint64(&m.stat_success)
	s.stat_failure = atomic.LoadUint64(&m.stat_failure)
	return
}

func NewMaster(url string, concurrency int, max_retry uint64) *Master {
	sentry, _ := raven.NewClient(SENTRY_DNS)
	return &Master{
		url:         url,
		tubes:       make([]string, 0),
		concurrency: concurrency,
		callbacks:   make(map[string]JobCallback),
		handlers:    make(map[string]JobHandler),
		timeouts:    make(map[string]time.Duration),
		max_retry:   max_retry,
		sentry:      sentry,
	}
}

func (m *Master) SetSentry(sentry *raven.Client) {
	m.sentry = sentry
}

func (m *Master) RegisterHandler(name string, hfn JobHandler, cfn JobCallback, to time.Duration) {
	m.tubes = append(m.tubes, name)
	m.handlers[name] = hfn
	m.callbacks[name] = cfn
	m.timeouts[name] = to
	return
}

func (m *Master) Start() (err error) {
	m.mg.Add(1)

	m.job = make(chan *Job, 2)
	m.quit = make(chan bool, m.concurrency)

	m.workers = make([]*Worker, m.concurrency)

	logger.Infof("master: starting %d workers...", m.concurrency)

	for i := 0; i < m.concurrency; i++ {
		if m.workers[i], err = NewWorker(m, i); err != nil {
			return Error("unable to start worker: %s", err)
		}
	}
	for i := 0; i < m.concurrency; i++ {
		go m.workers[i].run()
	}

	logger.Info("master: ready")

	return
}

func (m *Master) Stop() (err error) {
	logger.Infof("master: stopping %d workers...", m.concurrency)

	for i := 0; i < m.concurrency; i++ {
		m.quit <- true
	}

	m.wg.Wait()
	logger.Infof("master: %d workers stopped", m.concurrency)

	m.mg.Done()
	logger.Info("master: stopped")

	return
}

func (m *Master) Wait() {
	m.mg.Wait()
}
