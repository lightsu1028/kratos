package ewma

import (
	"container/list"
	"context"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/selector"
)

const (
	// The mean lifetime of `cost`, it reaches its half-life after Tau*ln(2).
	tau = int64(time.Millisecond * 600)
	// if statistic not collected,we add a big lag penalty to endpoint
	penalty = uint64(time.Second * 10)
)

var (
	_ selector.WeightedNode        = (*Node)(nil)
	_ selector.WeightedNodeBuilder = (*Builder)(nil)
)

// Node is endpoint instance
type Node struct {
	selector.Node

	// client statistic data
	lag       int64
	success   uint64
	inflight  int64
	inflights *list.List
	// last collected timestamp
	stamp     int64
	predictTs int64
	predict   int64
	// request number in a period time
	reqs int64
	// last lastPick timestamp
	lastPick int64

	errHandler func(err error) (isErr bool)
	lk         sync.RWMutex
}

// Builder is ewma node builder.
type Builder struct {
	ErrHandler func(err error) (isErr bool)
}

// Build create a weighted node.
func (b *Builder) Build(n selector.Node) selector.WeightedNode {
	s := &Node{
		Node:       n,
		lag:        0,
		success:    1000,
		inflight:   1,
		inflights:  list.New(),
		errHandler: b.ErrHandler,
	}
	return s
}

func (n *Node) health() uint64 {
	return atomic.LoadUint64(&n.success)
}

func (n *Node) load() (load uint64) {
	now := time.Now().UnixNano()
	avgLag := atomic.LoadInt64(&n.lag)
	lastPredictTs := atomic.LoadInt64(&n.predictTs)
	predictInterval := avgLag / 5
	// 5ms=<predictInterval<=200ms
	if predictInterval < int64(time.Millisecond*5) {
		predictInterval = int64(time.Millisecond * 5)
	}
	if predictInterval > int64(time.Millisecond*200) {
		predictInterval = int64(time.Millisecond * 200)
	}
	// 距离上一次计算权重
	if now-lastPredictTs > predictInterval && atomic.CompareAndSwapInt64(&n.predictTs, lastPredictTs, now) {
		var (
			total   int64
			count   int
			predict int64
		)
		n.lk.RLock()
		// 统计当前客户端发出去还未应答的请求延迟响应时间总数
		first := n.inflights.Front()
		for first != nil {
			lag := now - first.Value.(int64)
			if lag > avgLag {
				count++
				total += lag
			}
			first = first.Next()
		}
		// 当前客户端发出的正在处理的请求个数超过半数的lantency大于历史平均lantency
		// 计算这些高延时的请求平均lantency
		if count > (n.inflights.Len()/2 + 1) {
			predict = total / int64(count)
		}
		n.lk.RUnlock()
		atomic.StoreInt64(&n.predict, predict)
	}

	if avgLag == 0 {
		// penalty is the penalty value when there is no data when the node is just started.
		// The default value is 1e9 * 10
		load = penalty * uint64(atomic.LoadInt64(&n.inflight))
		return
	}
	predict := atomic.LoadInt64(&n.predict)
	// 若半数以上正在处理的请求的平均lantency大于历史平均lantency，取这些半数处理中请求的平均lantency作为负载测算lantency
	if predict > avgLag {
		avgLag = predict
	}
	load = uint64(avgLag) * uint64(atomic.LoadInt64(&n.inflight))
	return
}

// Pick pick a node.
func (n *Node) Pick() selector.DoneFunc {
	now := time.Now().UnixNano()
	atomic.StoreInt64(&n.lastPick, now)
	atomic.AddInt64(&n.inflight, 1)
	atomic.AddInt64(&n.reqs, 1)
	n.lk.Lock()
	e := n.inflights.PushBack(now)
	n.lk.Unlock()
	return func(ctx context.Context, di selector.DoneInfo) {
		n.lk.Lock()
		n.inflights.Remove(e)
		n.lk.Unlock()
		atomic.AddInt64(&n.inflight, -1)

		now := time.Now().UnixNano()
		// get moving average ratio w
		stamp := atomic.SwapInt64(&n.stamp, now)
		// 请求距离上次的访问时间 td越小 访问频率越高 td越大 访问频率越低
		// https://zhuanlan.zhihu.com/p/401118943
		// https://exceting.github.io/2020/08/13/%E8%B4%9F%E8%BD%BD%E5%9D%87%E8%A1%A1-P2C%E7%AE%97%E6%B3%95/
		// td越小 说明当前客户端访问服务越频繁，说明服务节点网络负载升高了, 我们想监测到此时节点处理请求的耗时(侧面反映了节点的负载情况), 我们就相应的调小β，β越小，EWMA值 就越接近本次耗时，进而迅速监测到网络毛刺;
		// 当请求较为不频繁时, 我们就相对的调大β值。这样计算出来的 EWMA值 越接近平均值
		td := now - stamp
		if td < 0 {
			td = 0
		}
		// (e^(-td)-1)/tau
		// 实际td越大 w越小
		// 此处有点奇怪？ td越小 w反而越大 不符合ewma的β设定
		// td越大 w越小
		// 如果硬要使用下面这个衰减函数 那td应该表示为当次请求的lantency td越大 说明节点负载越高  w就越小越能监测毛刺;td越小 说明节点负载越小 w就越大越能平滑
		w := math.Exp(float64(-td) / float64(tau))

		start := e.Value.(int64)
		lag := now - start
		if lag < 0 {
			lag = 0
		}
		oldLag := atomic.LoadInt64(&n.lag)
		if oldLag == 0 {
			w = 0.0
		}
		lag = int64(float64(oldLag)*w + float64(lag)*(1.0-w))
		atomic.StoreInt64(&n.lag, lag)

		success := uint64(1000) // error value ,if error set 1
		if di.Err != nil {
			if n.errHandler != nil && n.errHandler(di.Err) {
				success = 0
			}
			var netErr net.Error
			if errors.Is(context.DeadlineExceeded, di.Err) || errors.Is(context.Canceled, di.Err) ||
				errors.IsServiceUnavailable(di.Err) || errors.IsGatewayTimeout(di.Err) || errors.As(di.Err, &netErr) {
				success = 0
			}
		}
		oldSuc := atomic.LoadUint64(&n.success)
		success = uint64(float64(oldSuc)*w + float64(success)*(1.0-w))
		atomic.StoreUint64(&n.success, success)
	}
}

// Weight is node effective weight.
func (n *Node) Weight() (weight float64) {
	weight = float64(n.health()*uint64(time.Second)) / float64(n.load())
	return
}

func (n *Node) PickElapsed() time.Duration {
	return time.Duration(time.Now().UnixNano() - atomic.LoadInt64(&n.lastPick))
}

func (n *Node) Raw() selector.Node {
	return n.Node
}
