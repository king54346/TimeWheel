package time_wheel

import (
	"container/list"
	"log/slog"
	"sync"
	"time"
)

type taskElement struct {
	task   func()
	circle int
	key    string
	delay  time.Duration
}

// 环状数组中的位置
type location struct {
	pos   int           //时间轮位置
	etask *list.Element //list链表中的任务节点
}

type TimeWheel struct {
	sync.Once
	interval time.Duration //时间间隔
	ticker   *time.Ticker  //定时器
	slots    []*list.List  //tw数组

	stopChannel  chan struct{}     //关闭时间轮
	addTaskCh    chan *taskElement //添加任务
	removeTaskCh chan string       //删除任务

	curSlot int                  //当前位置
	timer   map[string]*location //key是任务的key，value是任务节点 用于更快的查找任务位置

	LongTermMap map[int64]*list.List
}

func NewTimeWheel(slotNum int, interval time.Duration) *TimeWheel {
	if slotNum <= 0 {
		slotNum = 10
	}
	if interval <= 0 {
		interval = time.Second
	}

	t := TimeWheel{
		interval:     interval,
		ticker:       time.NewTicker(interval),
		stopChannel:  make(chan struct{}),
		timer:        make(map[string]*location),
		slots:        make([]*list.List, slotNum),
		addTaskCh:    make(chan *taskElement),
		removeTaskCh: make(chan string),
	}
	t.initSlots()

	go t.run()
	return &t
}

func (t *TimeWheel) initSlots() {
	for i := 0; i < len(t.slots); i++ {
		t.slots[i] = list.New()
	}
}

func (t *TimeWheel) Stop() {
	t.Do(func() {
		t.ticker.Stop()
		close(t.stopChannel)
	})
}

func (t *TimeWheel) AddTask(delay time.Duration, task func(), key string) {
	if delay < 0 {
		return
	}
	t.addTaskCh <- &taskElement{
		delay: delay,
		task:  task,
		key:   key,
	}
}

func (t *TimeWheel) RemoveTask(key string) {
	t.removeTaskCh <- key
}

func (t *TimeWheel) run() {
	defer func() {
		if err := recover(); err != nil {
			// ...
		}
	}()

	for {
		select {
		case <-t.stopChannel:
			return
		case <-t.ticker.C:
			t.tickHandler()
		case task := <-t.addTaskCh:
			t.addTask(task)
		case removeKey := <-t.removeTaskCh:
			t.removeTask(removeKey)
		}
	}
}

// 执行定时任务
func (t *TimeWheel) tickHandler() {
	list := t.slots[t.curSlot]
	defer t.circularIncr()
	//开启异步协程执行
	t.execute(list)
}

func (t *TimeWheel) execute(l *list.List) {
	// 遍历每个 list
	for e := l.Front(); e != nil; {
		taskElement, _ := e.Value.(*taskElement)
		if taskElement.circle > 0 {
			taskElement.circle--
			e = e.Next()
			continue
		}

		// 执行任务
		go func() {
			defer func() {
				if err := recover(); err != nil {
					slog.Error("execute task error: %v", err)
				}
			}()
			taskElement.task()
		}()

		// 执行任务后，从时间轮中删除
		next := e.Next()
		l.Remove(e)
		delete(t.timer, taskElement.key)
		e = next
	}
}

// 获取当前时间轮的位置和圈数
func (t *TimeWheel) getPosAndCircle(d time.Duration) (int, int) {
	delay := int(d)
	cycle := delay / len(t.slots) / int(t.interval)
	pos := (t.curSlot + delay/int(t.interval)) % len(t.slots)
	return pos, cycle
}

func (t *TimeWheel) addTask(task *taskElement) {
	pos, circle := t.getPosAndCircle(task.delay)
	task.circle = circle

	e := t.slots[pos].PushBack(task)
	loc := &location{
		pos:   pos,
		etask: e,
	}
	//检查并移除旧任务
	if _, ok := t.timer[task.key]; ok {
		t.removeTask(task.key)
	}

	t.timer[task.key] = loc
}

// 删除任务
func (t *TimeWheel) removeTask(key string) {
	loc, ok := t.timer[key]
	if !ok {
		return
	}
	l := t.slots[loc.pos]
	l.Remove(loc.etask)
	delete(t.timer, key)
}

func (t *TimeWheel) circularIncr() {
	t.curSlot = (t.curSlot + 1) % len(t.slots)
}
