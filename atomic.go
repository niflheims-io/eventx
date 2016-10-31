package eventx

import "sync/atomic"

type AtomicBoolean struct {
	value int64
}

func NewAtomicBoolean(value bool) *AtomicBoolean {
	n := int64(0)
	if value {
		n = int64(1)
	}
	return &AtomicBoolean{value:n}
}

func (a *AtomicBoolean) CompareAndSet(old bool, new bool) bool {
	oldVal := int64(0)
	newVal := int64(0)
	if old {
		oldVal = int64(1)
	}
	if new {
		newVal = int64(1)
	}
	return atomic.CompareAndSwapInt64(&a.value, oldVal, newVal)
}

func (a *AtomicBoolean) Set(val bool) {
	for {
		if a.CompareAndSet(a.Get(), val) {
			break
		}
	}
}


func (a *AtomicBoolean) Get() bool {
	return a.value == int64(1)
}