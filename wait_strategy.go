package eventx

import (
	"runtime"
	"sync"
)

// WaitStrategy
type WaitStrategy interface {
	WaitFor(int64, *Serial, *Serial, SerialBarrier) (int64, error)
	SignalAllWhenBlocking()
}

const (
	SPIN_TRIES = 100
)

type YieldingWaitStrategy struct {

}

func (w *YieldingWaitStrategy) WaitFor(sequence int64, index *Serial, dependentSerial *Serial, barrier SerialBarrier) (int64, error) {
	availableSerial := dependentSerial.Get()
	counter := SPIN_TRIES
	for {
		if availableSerial < sequence {
			checkErr := barrier.CheckAlert()
			if checkErr != nil {
				return int64(0), checkErr
			}
			if counter == 0 {
				runtime.Gosched()
			}
			counter = counter - 1
			availableSerial = dependentSerial.Get()
		} else {
			break
		}
	}
	return availableSerial, nil
}

func (w *YieldingWaitStrategy) SignalAllWhenBlocking()  {}

type BlockingWaitStrategy struct {
	lock *sync.Mutex
	cond *sync.Cond
}

func NewBlockingWaitStrategy() *BlockingWaitStrategy {
	lock := new(sync.Mutex)
	cond := sync.NewCond(lock)
	return &BlockingWaitStrategy{
		lock:lock,
		cond:cond,
	}
}

func (w *BlockingWaitStrategy) WaitFor(serial int64, index *Serial, dependentSerial *Serial, barrier SerialBarrier) (int64, error) {
	if index.Get() < serial {
		w.lock.Lock()
		for {
			if index.Get() < serial {
				checkErr := barrier.CheckAlert()
				if checkErr != nil {
					w.lock.Unlock()
					return int64(0), checkErr
				}
				w.cond.Wait()
			} else {
				break
			}
		}
		w.lock.Unlock()
	}
	availableSerial := dependentSerial.Get()
	for {
		if availableSerial < serial {
			checkErr := barrier.CheckAlert()
			if checkErr != nil {
				return int64(0), checkErr
			}
			availableSerial = dependentSerial.Get()
		} else {
			break
		}
	}
	return availableSerial, nil
}

func (w *BlockingWaitStrategy) SignalAllWhenBlocking()  {
	w.lock.Lock()
	w.cond.Broadcast()
	w.lock.Unlock()
}
