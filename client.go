package workout

import (
	"fmt"
	"github.com/bububa/beanstalk"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var criticalErrors = []string{"EOF", "broken pipe"}

type Client struct {
	ReserveTimeout time.Duration
	conn           *beanstalk.Conn
	mu             *sync.Mutex
	stat_put       uint64
	stat_reserve   uint64
	stat_release   uint64
	ExitChan       chan struct{}
}

func NewClient(addr string, tubes []string) (client *Client, err error) {
	var conn *beanstalk.Conn
	if conn, err = beanstalk.Dial("tcp", addr); err != nil {
		return
	}

	conn.TubeSet = *beanstalk.NewTubeSet(conn, tubes...)

	client = &Client{
		conn:           conn,
		mu:             new(sync.Mutex),
		ReserveTimeout: time.Duration(5 * time.Second),
		ExitChan:       make(chan struct{}, 1),
	}

	return
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Put(job *Job) (id uint64, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	tube := &beanstalk.Tube{
		Conn: c.conn,
		Name: job.Tube,
	}

	id, err = tube.Put([]byte(job.Body), job.Priority, time.Duration(job.Delay), time.Duration(job.TimeToRun))
	if err != nil {
		atomic.AddUint64(&c.stat_put, 1)
	}

	return
}

func (c *Client) Reserve() (job *Job, found bool, err error) {
	var id uint64
	var body []byte
	var stats map[string]string

	c.mu.Lock()
	defer c.mu.Unlock()

	id, body, err = c.conn.TubeSet.Reserve(c.ReserveTimeout)
	if err != nil {
		for _, estr := range criticalErrors {
			if strings.Contains(fmt.Sprintf("%s", err), estr) {
				logger.Warn(err)
				os.Exit(1)
			}
		}

		found = false
		err = nil
		return
	}

	stats, err = c.conn.StatsJob(id)
	if err != nil {
		logger.Warn(err)
		return
	}

	job = new(Job)
	job.Id = id
	job.Body = string(body)
	job.Priority = uint32(parseInt(stats["pri"]))
	job.Tube = stats["tube"]
	job.Delay = time.Duration(parseInt(stats["delay"])) * time.Second
	job.TimeToRun = time.Duration(parseInt(stats["ttr"])) * time.Second
	job.Age = time.Duration(parseInt(stats["age"])) * time.Second
	job.Attempt = uint32(parseInt(stats["reserves"]))

	found = true

	atomic.AddUint64(&c.stat_reserve, 1)

	return
}

func (c *Client) Delete(job *Job) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	err = c.conn.Delete(job.Id)

	return
}

func (c *Client) Release(job *Job, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delay := time.Duration(0)

	if err != nil {
		delay = job.NextDelay()
	}

	c.conn.Release(job.Id, job.Priority, delay)

	atomic.AddUint64(&c.stat_release, 1)

	return
}

func (c *Client) StatsJob(jobId uint64) (map[string]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	stats, err := c.conn.StatsJob(jobId)
	return stats, err
}

func (c *Client) Exit() {
	c.Close()
	c.ExitChan <- struct{}{}
}

func (c *Client) Drop() {
	ticker := time.NewTicker(time.Duration(5) * time.Second)

	var job *Job
	var ok bool

	for {
		select {
		case <-c.ExitChan:
			ticker.Stop()
			return
		case <-ticker.C:
			go c.Stats()
		default:
		}
		if job, ok, _ = c.Reserve(); !ok {
			continue
		}
		c.Delete(job)
	}
}

func (c *Client) Stats() {
	c.mu.Lock()
	defer c.mu.Unlock()

	stats, _ := c.conn.Stats()
	currentJobs := uint32(parseInt(stats["current-jobs-ready"]))
	logger.Debug("current ready jobs:%d", currentJobs)
	if currentJobs == 0 {
		c.Exit()
	}
}
