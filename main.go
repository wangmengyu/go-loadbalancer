package main

import (
	"container/heap"
	"fmt"
	"math/rand"
	"time"
)

const nRequester = 100
const nWorker = 10

// 模拟一个操作，会sleep 一会儿并且输出N秒
func op() int {
	n := rand.Int63n(int64(time.Second))
	time.Sleep(time.Duration(nWorker * n))
	return int(n)
}

//请求
type Request struct {
	fn func() int //需要执行的操作
	c  chan int   //返回结果的channel
}

//请求器,Request的发生器：
func requester(work chan Request) {
	c := make(chan int)
	for {
		time.Sleep(time.Duration(rand.Int63n(int64(nWorker * 2 * time.Second))))
		work <- Request{
			fn: op,
			c:  c,
		}
		//一旦完成worker内的等到操作就进入下一轮
		<-c
	}
}

/**
worker 对象，包含一个request channel， 和待处理数量，和当前的index
*/
type Worker struct {
	requests chan Request
	pending  int
	i        int
}

/**
不断从自己的request通道，获得需要操作的请求，并执行里面的func，并等待通知结果告诉出去完成了
*/
func (w *Worker) work(done chan *Worker) {
	for r := range w.requests {
		r.c <- r.fn()
		done <- w
	}
}

//实现了堆操作的worker池子
type Pool []*Worker

//堆得长度
func (p Pool) Len() int { return len(p) }

//比较哪个worker的负载小
func (p Pool) Less(i, j int) bool {
	return p[i].pending < p[j].pending
}

//交换两个节点的值
func (p *Pool) Swap(i, j int) {
	a := *p
	a[i], a[j] = a[j], a[i]
	a[i].i = i
	a[j].i = j
}

func (p *Pool) Push(x interface{}) {
	a := *p
	n := len(a)
	a = a[0 : n+1]
	w := x.(*Worker)
	a[n] = w
	w.i = n
	*p = a
}

func (p *Pool) Pop() interface{} {
	a := *p
	*p = a[0 : len(a)-1]
	w := a[len(a)-1]
	w.i = -1 // for safety
	return w
}

type Balancer struct {
	pool Pool
	done chan *Worker
	i    int
}

func NewBalancer() *Balancer {
	done := make(chan *Worker, nWorker)
	b := &Balancer{make(Pool, 0, nWorker), done, 0}
	for i := 0; i < nWorker; i++ {
		w := &Worker{requests: make(chan Request, nRequester)}
		heap.Push(&b.pool, w)
		go w.work(b.done)
	}
	return b
}

//均衡：读取需要处理的请求分配给worker,或者看有没有完成的，将他设置成完成
func (b *Balancer) balance(work chan Request) {
	for {
		select {
		case req := <-work:
			b.dispatch(req)
		case w := <-b.done:
			b.completed(w)
		}
		b.print()
	}
}

/**
  打印当前每个worker具有的待处理数，和
*/
func (b *Balancer) print() {
	sum := 0
	sumsq := 0
	for _, w := range b.pool {
		fmt.Printf("%d ", w.pending)
		sum += w.pending
		sumsq += w.pending * w.pending
	}
	avg := float64(sum) / float64(len(b.pool))
	variance := float64(sumsq)/float64(len(b.pool)) - avg*avg
	fmt.Printf(" %.2f %.2f\n", avg, variance)
}

//调度器执行纷发给request给负载最小的worker
func (b *Balancer) dispatch(req Request) {
	w := heap.Pop(&b.pool).(*Worker)
	w.requests <- req
	w.pending++
	//	fmt.Printf("started %p; now %d\n", w, w.pending)
	heap.Push(&b.pool, w)
}

//worker执行完成，修改待处理数字，并且将节点从堆移除后再次推入
func (b *Balancer) completed(w *Worker) {
	if false {
		w.pending--
		return
	}

	w.pending--
	//	fmt.Printf("finished %p; now %d\n", w, w.pending)
	heap.Remove(&b.pool, w.i)
	heap.Push(&b.pool, w)
}

func main() {
	work := make(chan Request)
	for i := 0; i < nRequester; i++ {
		go requester(work)
	}
	NewBalancer().balance(work)
}
