package delayQueue

import (
	"container/heap"
	"container/list"
	"sync"
	"time"
)

// DelayedItem 是队列中的元素，需要实现heap.Interface接口
type DelayedItem[T any] struct {
	value    T     // 保存值
	priority int64 // 优先级，使用时间戳表示
	index    int   // 元素在堆中的索引
}

// DelayQueue 是一个延迟队列
type DelayQueue[T any] struct {
	mu              sync.Mutex
	cond            *sync.Cond
	Complete        *list.List
	waitingForAddCh chan *DelayedItem[T]
	// stopCh lets us signal a shutdown to the waiting loop
	stopCh   chan struct{}
	stopOnce sync.Once
	close    bool
}

// delayedQueue 实现了heap.Interface接口，并存储DelayedItem
type delayedQueue[T any] []*DelayedItem[T]

func (dq delayedQueue[T]) Len() int { return len(dq) }

func (dq delayedQueue[T]) Less(i, j int) bool {
	return dq[i].priority < dq[j].priority
}

func (dq delayedQueue[T]) Swap(i, j int) {
	dq[i], dq[j] = dq[j], dq[i]
	dq[i].index = i
	dq[j].index = j
}

func (dq *delayedQueue[T]) Push(x any) {
	n := len(*dq)
	item := x.(*DelayedItem[T])
	item.index = n
	*dq = append(*dq, item)
}

func (dq *delayedQueue[T]) Pop() any {
	old := *dq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*dq = old[0 : n-1]
	return item
}

func (dq delayedQueue[T]) Peek() any {
	return dq[0]
}

// NewDelayQueue 创建并返回一个新的DelayQueue
func NewDelayQueue[T any]() *DelayQueue[T] {
	dq := &DelayQueue[T]{}
	dq.waitingForAddCh = make(
		chan *DelayedItem[T],
		1000,
	)
	dq.mu = sync.Mutex{}
	dq.Complete = list.New()
	dq.cond = sync.NewCond(&dq.mu)
	dq.stopCh = make(chan struct{})
	dq.close = false
	go dq.waitingLoop()
	return dq
}

func (dq *DelayQueue[T]) ShutDown() {
	dq.stopOnce.Do(func() {
		close(dq.stopCh)
		dq.close = true
		dq.cond.Broadcast()
	})
}

func (dq *DelayQueue[T]) waitingLoop() {
	//用于在没有其他事件要等待时使用
	never := make(<-chan time.Time)
	waitingForQueue := &delayedQueue[T]{}
	heap.Init(waitingForQueue)
	// 用于快速查找是否相同元素的
	waitingEntryByData := map[any]*DelayedItem[T]{}
	//创建一个计时器，当等待队列头部的项目准备就绪时，该计时器到期
	var nextReadyAtTimer *time.Ticker
	start := time.Now()
	for {
		now := time.Now().UnixNano()
		//处理延迟
		for waitingForQueue.Len() > 0 {
			entry := waitingForQueue.Peek().(*DelayedItem[T])
			//需要延时
			if entry.priority > now {
				break
			}
			entry = heap.Pop(waitingForQueue).(*DelayedItem[T])
			dq.Complete.PushBack(entry)
			dq.cond.Broadcast()
			delete(waitingEntryByData, entry.value)
		}
		// Set up a wait for the first item's readyAt (if one exists)
		nextReadyAt := never
		if waitingForQueue.Len() > 0 {
			if nextReadyAtTimer != nil {
				nextReadyAtTimer.Stop()
			}
			// 计算等待第一个要添加元素的等待时间
			entry := waitingForQueue.Peek().(*DelayedItem[T])
			nextReadyAtTimer = time.NewTicker(time.Duration(entry.priority - now))
			nextReadyAt = nextReadyAtTimer.C
		}

		select {
		case <-dq.stopCh:
			println("finish")
			return
		case <-nextReadyAt:
			end := time.Now()
			duration := end.Sub(start) // continue the loop, which will add ready items
			println("delay", duration.Seconds())
		//获取放入 waitingForAddCh chan中的元素
		case waitEntry := <-dq.waitingForAddCh:
			now = time.Now().UnixNano()
			// 防止waitingchannel阻塞时间过长
			if waitEntry.priority > now {
				insert(waitingForQueue, waitingEntryByData, waitEntry)
			} else {
				dq.Complete.PushBack(waitEntry)
				dq.cond.Broadcast()
			}
			//排空waitingForAddCh 通道
			drained := false
			for !drained {
				select {
				case waitEntry := <-dq.waitingForAddCh:
					if waitEntry.priority > now {
						insert(waitingForQueue, waitingEntryByData, waitEntry)
					} else {
						dq.Complete.PushBack(waitEntry)
						dq.cond.Broadcast()
					}
				default:
					drained = true
				}
			}
		}
	}
}

func insert[T any](q *delayedQueue[T], knownEntries map[any]*DelayedItem[T], entry *DelayedItem[T]) {
	// 如果条目已经存在，则仅在比存在时间更短的情况下更新时间，不会推迟原有的时间
	existing, exists := knownEntries[entry.value]
	if exists {
		if existing.priority > entry.priority {
			existing.priority = entry.priority
			heap.Fix(q, existing.index)
		}

		return
	}

	heap.Push(q, entry)
	knownEntries[entry.value] = entry
}

func (q *DelayQueue[T]) AddAfter(item T, duration time.Duration) {
	if duration < 0 {
		return
	}

	select {
	case <-q.stopCh:
		// unblock if ShutDown() is called
	case q.waitingForAddCh <- &DelayedItem[T]{
		value:    item,
		priority: time.Now().Add(duration).UnixNano()}:
		println("addTask", item)
	}
}

func (q *DelayQueue[T]) Poll() T {
	q.mu.Lock()
	defer q.mu.Unlock()
	front := q.Complete.Front()
	var zero T
	if q.close {
		return zero
	}
	if q.Complete.Len() == 0 {
		return zero
	}
	value := front.Value
	q.Complete.Remove(front)
	return value.(*DelayedItem[T]).value
}

func (q *DelayQueue[T]) Take() T {
	q.mu.Lock()
	defer q.mu.Unlock()
	var zero T
	if q.close {
		return zero
	}
	for q.Complete.Len() == 0 {
		q.cond.Wait()
		if q.close {
			return zero
		}
	}
	front := q.Complete.Front()
	value := front.Value
	q.Complete.Remove(front)
	return value.(*DelayedItem[T]).value
}

func main() {
	dq := NewDelayQueue[string]()
	go func() {
		println("Take", dq.Take())
	}()
	go func() {
		println("Take", dq.Take())
		dq.ShutDown()
	}()
	go func() {
		println("Take", dq.Take())
	}()
	go func() {
		println("Take", dq.Take())
	}()
	// 添加元素到延迟队列
	dq.AddAfter("task1", 5*time.Second)
	//for i := 0; i < 10000; i++ {
	//	item := fmt.Sprintf("task %d", i)
	//	go func() {
	//		dq.AddAfter(item, 1*time.Second)
	//	}()
	//}
	dq.AddAfter("task2", 7*time.Second)
	dq.AddAfter("task3", 11*time.Second)

	time.Sleep(12 * time.Second)

	dq.AddAfter("task4", 2*time.Second)
	println("Take", dq.Take())
}
