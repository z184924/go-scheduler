package scheduler

import (
	"github.com/robfig/cron"
	"log"
	"runtime"
	"sort"
	"sync"
	"time"
)

var instance *Scheduler
var once sync.Once

type Scheduler struct {
	cron    *Cron
	taskMap map[string]*TaskInfo
}

type Task interface {
	Run(parameter map[string]interface{}) bool
}

type TaskInfo struct {
	Task
	TaskName     string
	parameterMap map[string]interface{}
	spec         string
	RunCount     int
	SuccessCount int
	AverageTime  int64
	LongestTime  int64
	RunFlag      bool
	entry        *Entry
}

/**
GetInstance
*/
func GetInstance() *Scheduler {
	once.Do(func() {
		instance = &Scheduler{
			cron:    newWithLocation(time.Now().Location()),
			taskMap: make(map[string]*TaskInfo),
		}
		instance.cron.start()
	})
	return instance
}

/**
AddTask
@param task Task interface
@param taskName taskName(Unique)
@param Parameter Method Parameter
@param spec cron(eg: 0 0 1 * * ?)
*/
func (p *Scheduler) AddTask(task Task, taskName string, parameterMap map[string]interface{}, spec string) error {
	info := &TaskInfo{
		task,
		taskName,
		parameterMap,
		spec,
		0,
		0,
		0,
		0,
		true,
		nil,
	}
	p.taskMap[taskName] = info
	return p.cron.addJob(info, spec, funcJob(func() {
		startTime := time.Now().UnixNano()
		flag := task.Run(parameterMap)
		endTime := time.Now().UnixNano()
		info.RunCount++
		usedTime := (endTime - startTime) / int64(time.Millisecond)
		if usedTime > info.LongestTime {
			info.LongestTime = usedTime
		}
		info.AverageTime = ((info.AverageTime * int64(info.RunCount-1)) + usedTime) / int64(info.RunCount)
		if flag {
			info.SuccessCount++
		}
	}))
}

/**
RemoveTask
@param taskName
*/
func (p *Scheduler) RemoveTask(taskName string) bool {
	task := p.GetTask(taskName)
	if task == nil {
		return false
	}
	for i, entity := range p.cron.entries {
		if entity == task.entry {
			p.cron.entries = append(p.cron.entries[:i], p.cron.entries[i+1:]...)
			break
		}
	}
	delete(p.taskMap, taskName)
	return true
}

/**
GetTask
@param taskName
*/
func (p *Scheduler) GetTask(taskName string) *TaskInfo {
	taskInfo := p.taskMap[taskName]
	return taskInfo
}

/**
StopTask
@param taskName
*/
func (p *Scheduler) StopTask(taskName string) bool {
	taskInfo, ok := p.taskMap[taskName]
	if ok {
		taskInfo.entry.runFlag = false
		taskInfo.RunFlag = false
		return true
	} else {
		return false
	}
}

/**
StartTask
@param taskName
*/
func (p *Scheduler) StartTask(taskName string) bool {
	taskInfo, ok := p.taskMap[taskName]
	if ok {
		taskInfo.entry.runFlag = true
		taskInfo.RunFlag = true
		return true
	} else {
		return false
	}
}

/**
GetTaskMap
*/
func (p *Scheduler) GetTaskMap() map[string]*TaskInfo {
	return p.taskMap
}

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	entries  []*Entry
	stop     chan struct{}
	add      chan *Entry
	snapshot chan []*Entry
	running  bool
	ErrorLog *log.Logger
	location *time.Location
}

type Job interface {
	Run()
}

// The Schedule describes a job's duty cycle.
type Schedule interface {
	// Return the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(time.Time) time.Time
}

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
	// The schedule on which this job should be run.
	Schedule Schedule

	// The next time the job will run. This is the zero time if Cron has not been
	// started or this entry's schedule is unsatisfiable
	Next time.Time

	// The last time this job was run. This is the zero time if the job has never
	// been run.
	Prev time.Time

	// The Job to run.
	Job Job

	//If runFlag is false , job will stop
	runFlag bool
}

// byTime is a wrapper for sorting the entry array by time
// (with zero time at the end).
type byTime []*Entry

func (s byTime) Len() int      { return len(s) }
func (s byTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byTime) Less(i, j int) bool {
	// Two zero times should return false.
	// Otherwise, zero is "greater" than any other time.
	// (To sort it at the end of the list.)
	if s[i].Next.IsZero() {
		return false
	}
	if s[j].Next.IsZero() {
		return true
	}
	return s[i].Next.Before(s[j].Next)
}

// NewWithLocation returns a new Cron job runner.
func newWithLocation(location *time.Location) *Cron {
	return &Cron{
		entries:  nil,
		add:      make(chan *Entry),
		stop:     make(chan struct{}),
		snapshot: make(chan []*Entry),
		running:  false,
		ErrorLog: nil,
		location: location,
	}
}

// A wrapper that turns a func() into a cron.Job
type funcJob func()

func (f funcJob) Run() { f() }

// AddJob adds a Job to the Cron to be run on the given schedule.
func (c *Cron) addJob(taskInfo *TaskInfo, spec string, cmd Job) error {
	schedule, err := cron.Parse(spec)
	if err != nil {
		return err
	}
	c.schedule(taskInfo, schedule, cmd)
	return nil
}

// Schedule adds a Job to the Cron to be run on the given schedule.
func (c *Cron) schedule(taskInfo *TaskInfo, schedule Schedule, cmd Job) {
	entry := &Entry{
		Schedule: schedule,
		Job:      cmd,
		runFlag:  true,
	}
	taskInfo.entry = entry
	if !c.running {
		c.entries = append(c.entries, entry)
		return
	}

	c.add <- entry
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []*Entry {
	if c.running {
		c.snapshot <- nil
		x := <-c.snapshot
		return x
	}
	return c.entrySnapshot()
}

// Location gets the time zone location
func (c *Cron) Location() *time.Location {
	return c.location
}

// Start the cron go-scheduler in its own go-routine, or no-op if already started.
func (c *Cron) start() {
	if c.running {
		return
	}
	c.running = true
	go c.run()
}

func (c *Cron) runWithRecovery(j Job) {
	defer func() {
		if r := recover(); r != nil {
			const size = 64 << 10
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			c.logf("cron: panic running job: %v\n%s", r, buf)
		}
	}()
	j.Run()
}

// Run the go-scheduler. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run() {
	// Figure out the next activation times for each entry.
	now := c.now()
	for _, entry := range c.entries {
		entry.Next = entry.Schedule.Next(now)
	}

	for {
		// Determine the next entry to run.
		sort.Sort(byTime(c.entries))

		var timer *time.Timer
		if len(c.entries) == 0 || c.entries[0].Next.IsZero() {
			// If there are no entries yet, just sleep - it still handles new entries
			// and stop requests.
			timer = time.NewTimer(100000 * time.Hour)
		} else {
			timer = time.NewTimer(c.entries[0].Next.Sub(now))
		}

		for {
			select {
			case now = <-timer.C:
				now = now.In(c.location)
				// Run every entry whose next time was less than now
				for _, e := range c.entries {
					if !e.runFlag {
						continue
					}
					if e.Next.After(now) || e.Next.IsZero() {
						break
					}
					go c.runWithRecovery(e.Job)
					e.Prev = e.Next
					e.Next = e.Schedule.Next(now)
				}

			case newEntry := <-c.add:
				timer.Stop()
				now = c.now()
				newEntry.Next = newEntry.Schedule.Next(now)
				c.entries = append(c.entries, newEntry)

			case <-c.snapshot:
				c.snapshot <- c.entrySnapshot()
				continue

			case <-c.stop:
				timer.Stop()
				return
			}

			break
		}
	}
}

// Logs an error to stderr or to the configured error log
func (c *Cron) logf(format string, args ...interface{}) {
	if c.ErrorLog != nil {
		c.ErrorLog.Printf(format, args...)
	} else {
		log.Printf(format, args...)
	}
}

// Stop stops the cron go-scheduler if it is running; otherwise it does nothing.
func (c *Cron) Stop() {
	if !c.running {
		return
	}
	c.stop <- struct{}{}
	c.running = false
}

// entrySnapshot returns a copy of the current cron entry list.
func (c *Cron) entrySnapshot() []*Entry {
	entries := []*Entry{}
	for _, e := range c.entries {
		entries = append(entries, &Entry{
			Schedule: e.Schedule,
			Next:     e.Next,
			Prev:     e.Prev,
			Job:      e.Job,
			runFlag:  e.runFlag,
		})
	}
	return entries
}

// now returns current time in c location
func (c *Cron) now() time.Time {
	return time.Now().In(c.location)
}
