package workout

import (
	"sync"
	"sync/atomic"
	"time"
)

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
	client         *Client
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

func NewMaster(url string, tubes []string, concurrency int, max_retry uint64) *Master {
	client, err := NewClient(url, tubes)
	if err != nil {
		logger.Warn(err)
	}
	return &Master{
		url:         url,
		tubes:       tubes,
		client:      client,
		concurrency: concurrency,
		callbacks:   make(map[string]JobCallback),
		handlers:    make(map[string]JobHandler),
		timeouts:    make(map[string]time.Duration),
		max_retry:   max_retry,
	}
}

func (m *Master) RegisterHandler(name string, hfn JobHandler, cfn JobCallback, to time.Duration) {
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

	logger.Debug("master: starting %d workers...", m.concurrency)

	for i := 0; i < m.concurrency; i++ {
		m.workers[i] = NewWorker(m, i)
		go m.workers[i].run()
	}

	logger.Debug("master: ready")

	return
}

func (m *Master) Stop() (err error) {
	logger.Debug("master: stopping %d workers...", m.concurrency)

	for i := 0; i < m.concurrency; i++ {
		m.quit <- true
	}

	m.wg.Wait()
	logger.Debug("master: %d workers stopped", m.concurrency)

	m.mg.Done()
	logger.Debug("master: stopped")

	return
}

func (m *Master) Wait() {
	m.mg.Wait()
}
