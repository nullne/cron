package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	maxDuration time.Duration = 1<<63 - 1
)

var (
	ErrCronIsClosing    = errors.New("cron is closing")
	ErrInvalidOperation = errors.New("invalid operation")
	ErrJobNotFound      = errors.New("job not found")
)

func echo1() {
	fmt.Println("running echo 1 at", time.Now())
}

func echo2() {
	fmt.Println("running echo 2 at", time.Now())
}

func main() {
	c := NewCron()
	c.AddJob(echo1, "*/1 * * * *")
	c.AddJob(echo2, "*/2 * * * *")
	c.AddJob(echo2, "*/2 * */3 * 4")
	ch := make(chan int)
	<-ch

	// test cron expression
	// exp := "1 2 */2 3 3,4"
	// fmt.Println(exp)
	// s, err := newSchedule(exp)
	// if err != nil {
	// 	panic(err)
	// }
	// now := time.Now()
	// fmt.Println(now)
	// for i := 0; i < 10; i++ {
	// 	now = s.nextFrom(now)
	// 	fmt.Println(now, now.Weekday())
	// }
}

type Cron struct {
	id        int
	jobs      map[int]*job
	planTable jobPlanTable

	operation chan jobOperation
	wg        sync.WaitGroup
	done      chan struct{}
}

type jobOperation struct {
	deleteJob int
	newJob    *job
	response  chan error
}

func fnWrapper(wg *sync.WaitGroup, fn func()) {
	defer wg.Done()
	fn()
}

func NewCron() *Cron {
	c := Cron{
		jobs:      make(map[int]*job),
		planTable: make(jobPlanTable, 0, 1),
		operation: make(chan jobOperation),
		done:      make(chan struct{}),
	}
	c.wg.Add(1)
	go c.run()
	return &c
}

func (c *Cron) run() {
	defer c.wg.Done()
	var drainNeeded bool
	var timer *time.Timer
	for {
		fmt.Println("planTable: ", c.planTable)
		select {
		case <-c.done:
			return
		default:
		}

		select {
		case operation := <-c.operation:
			c.handleOperation(operation)
			continue
		default:
		}

		if len(c.planTable) == 0 {
			timer = time.NewTimer(maxDuration)
		} else {
			timer = time.NewTimer(c.planTable[0].when.Sub(time.Now()))
		}

		select {
		case operation := <-c.operation:
			drainNeeded = true
			c.handleOperation(operation)
		case <-timer.C:
			drainNeeded = false
			c.runNearestJob()
		}

		if drainNeeded {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
	}
}

func (c *Cron) runNearestJob() {
	if len(c.planTable) == 0 {
		return
	}
	p := c.planTable.pop()
	if p.when.IsZero() {
		return
	}

	j, ok := c.jobs[p.id]
	if !ok {
		return
	}

	c.wg.Add(1)
	go fnWrapper(&(c.wg), j.fn)
	c.planTable.add(j.next(p.when))
}

func (c *Cron) handleOperation(op jobOperation) {
	var err error
	switch {
	case op.deleteJob > 0:
		_, ok := c.jobs[op.deleteJob]
		if ok {
			delete(c.jobs, op.deleteJob)
		} else {
			err = ErrJobNotFound
		}
	case op.newJob != nil:
		c.id += 1
		op.newJob.id = c.id
		c.jobs[c.id] = op.newJob
		fmt.Printf("add job %d  at %v\n", c.id, time.Now())
		c.planTable.add(op.newJob.next(time.Now()))
	default:
		err = ErrInvalidOperation
	}
	op.response <- err
}

// AddJob adds fn as a job scheduled by s, return the job id
func (c *Cron) AddJob(fn func(), s string) (int, error) {
	sched, err := newSchedule(s)
	if err != nil {
		return 0, err
	}
	j := job{
		fn:       fn,
		schedule: sched,
	}
	op := jobOperation{
		newJob:   &j,
		response: make(chan error, 1),
	}
	select {
	case <-c.done:
		return 0, ErrCronIsClosing
	case c.operation <- op:
	}
	select {
	case err := <-op.response:
		return j.id, err
	case <-c.done:
		return 0, ErrCronIsClosing
	}
}

// DeleteJob deletes a job by id s
func (c *Cron) DeleteJob(id int) {
}

func (c *Cron) Stop() {
	close(c.done)
	c.wg.Wait()
}

type job struct {
	id       int
	fn       func()
	schedule schedule
}

func (j job) next(from time.Time) jobPlan {
	return jobPlan{
		id:   j.id,
		when: j.schedule.nextFrom(from),
	}
}

type jobPlan struct {
	id   int
	when time.Time
}

func (p jobPlan) String() string {
	return fmt.Sprintf("id: %d, when: %s", p.id, p.when.String())
}

type jobPlanTable []jobPlan

func (t jobPlanTable) leftChild(cur int) int {
	return (cur+1)*2 - 1
}

func (t jobPlanTable) rightChild(cur int) int {
	return (cur + 1) * 2
}

func (t jobPlanTable) parent(cur int) int {
	return (cur - 1) / 2
}

func (t *jobPlanTable) add(p jobPlan) {
	*t = append(*t, p)
	cur := len(*t) - 1
	for cur != 0 {
		p := t.parent(cur)
		if !(*t)[cur].when.Before((*t)[p].when) {
			break
		}
		(*t)[cur], (*t)[p] = (*t)[p], (*t)[cur]
		cur = p
	}
}

func (t *jobPlanTable) pop() jobPlan {
	if len(*t) == 0 {
		return jobPlan{}
	}
	r := (*t)[0]
	p := len(*t) - 1
	(*t)[0], (*t)[p] = (*t)[p], (*t)[0]
	(*t) = (*t)[:p]
	t.heapify(0)
	return r
}

func (t *jobPlanTable) heapify(n int) {
	m := len(*t)
	for n < m/2 {
		p := t.leftChild(n)
		if p < m && (*t)[p].when.Before((*t)[n].when) {
			(*t)[n], (*t)[p] = (*t)[p], (*t)[n]
			n = p
			continue
		}
		p = t.rightChild(n)
		if p < m && (*t)[p].when.Before((*t)[n].when) {
			(*t)[n], (*t)[p] = (*t)[p], (*t)[n]
			n = p
			continue
		}
		return
	}
}

// * any value
// , value list
// - range of value
// / step value
type scheduleUnit struct {
	min, max int
	value    int
	any      bool
	list     []int
	ranges   []int
	step     int
}

func (s scheduleUnit) validate() error {
	if s.value != -1 {
		if s.value < s.min {
			return errors.New("smaller than min")
		}
		if s.value > s.max {
			return errors.New("larger than max")
		}
	}
	for _, l := range s.list {
		if l < s.min {
			return errors.New("smaller than min")
		}
		if l > s.max {
			return errors.New("larger than max")
		}
	}
	for _, l := range s.ranges {
		if l < s.min {
			return errors.New("smaller than min")
		}
		if l > s.max {
			return errors.New("larger than max")
		}
	}
	return nil
}

func parseScheduleRange(s string) (r []int, err error) {
	values := strings.Split(s, "-")
	if len(values) != 2 {
		return r, errors.New("must be 2")
	}
	ranges := make([]int, 2)
	for i, v := range values {
		iv, err := strconv.Atoi(v)
		if err != nil {
			return r, err
		}
		ranges[i] = iv
	}
	return ranges, nil
}

func parseScheduleList(s string) ([]int, error) {
	values := strings.Split(s, ",")
	list := make([]int, len(values))
	for i, v := range values {
		iv, err := strconv.Atoi(v)
		if err != nil {
			return nil, err
		}
		list[i] = iv
	}
	return list, nil
}

func nilScheduleUnit() scheduleUnit {
	return scheduleUnit{
		value: -1,
		step:  -1,
	}
}

// cron expression not fully implemented
func newScheduleUnit(s string, min, max int) (u scheduleUnit, err error) {
	u = nilScheduleUnit()
	u.min = min
	u.max = max
	switch {
	case s == "*":
		u.any = true
	case strings.Contains(s, "/"):
		values := strings.Split(s, "/")
		if len(values) != 2 {
			return u, errors.New("must be 2")
		}
		u.step, err = strconv.Atoi(values[1])
		if err != nil {
			return u, err
		}
		switch {
		case values[0] == "*":
			u.any = true
		case strings.Contains(values[0], "-"):
			u.ranges, err = parseScheduleRange(values[0])
		case strings.Contains(values[0], ","):
			return u, errors.New("not implemented")
			// u.list, err = parseScheduleList(values[0])
		default:
			u.ranges = make([]int, 2)
			u.ranges[0], err = strconv.Atoi(values[0])
			u.ranges[1] = u.max
		}
	case strings.Contains(s, ","):
		u.list, err = parseScheduleList(s)
	case strings.Contains(s, "-"):
		u.ranges, err = parseScheduleRange(s)
	default:
		// fixed number
		u.value, err = strconv.Atoi(s)
	}
	if err != nil {
		return scheduleUnit{}, err
	}
	return u, nil
}

// return true means nothing more need to do
func (s scheduleUnit) next(cur int) (int, bool) {
	// fmt.Printf("%+v\n", s)
	switch {
	case s.step != -1:
		cur += s.step
		switch {
		case s.any:
			if cur > s.max {
				return s.min, false
			} else {
				return cur, true
			}
		case len(s.ranges) == 2:
			if cur > s.ranges[1] {
				return s.ranges[0], false
			} else {
				return cur, true
			}
			// case s.list != nil:
		}
	case s.value != -1:
		if s.value > cur {
			return s.value, true
		} else {
			return s.value, false
		}
	case s.any:
		cur += 1
		if cur > s.max {
			return s.min, false
		} else {
			return cur, true
		}
	case len(s.ranges) == 2:
		cur += 1
		if cur <= s.ranges[1] {
			return cur, true
		} else {
			return s.ranges[0], false
		}
	case s.list != nil:
		for _, v := range s.list {
			if v <= cur {
				continue
			}
			return v, true
		}
		return s.list[0], false
	}

	// impossible
	return 0, false
}

// https://crontab.guru
type schedule [5]scheduleUnit

var scheduleLimits = [5][2]int{
	{0, 59},
	{0, 23},
	{1, 31},
	{1, 12},
	{0, 6},
}

// minute hour monthDay month weekDay
// */1 * * * *
func newSchedule(s string) (sched schedule, err error) {
	ss := strings.Split(s, " ")
	if len(ss) != 5 {
		return schedule{}, errors.New("invalid schedule expression")
	}
	for i := range ss {
		sched[i], err = newScheduleUnit(ss[i], scheduleLimits[i][0], scheduleLimits[i][1])
		if err != nil {
			return schedule{}, err
		}
	}
	return sched, nil
}

func (s schedule) nextFrom(now time.Time) time.Time {
	year := now.Year()
	month := int(now.Month())
	day := now.Day()
	hour := now.Hour()
	minute := now.Minute()
	// firstday := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, now.Location())
	// lastday := firstday.AddDate(0, 1, 0).Add(time.Nanosecond * -1).Day()

	var done bool
	minute, done = s[0].next(minute)
	if !done {
		hour, done = s[1].next(hour)
	}
	next := time.Date(year, time.Month(month), day, hour, minute, 0, 0, now.Location())

	ws := make([]bool, 7)
	w := int(now.Weekday())
	for {
		w, _ = s[4].next(w)
		if ws[w] {
			break
		}
		ws[w] = true
	}

	for {
		if done && ws[int(next.Weekday())] {
			return next
		}
		day, done = s[2].next(day)
		if !done {
			month, done = s[3].next(month)
			if !done {
				year += 1
				done = true
			}
		}
		next = time.Date(year, time.Month(month), day, hour, minute, 0, 0, now.Location())
	}
}

func (s schedule) next() time.Time {
	return s.nextFrom(time.Now())
}
