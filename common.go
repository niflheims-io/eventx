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
	return atomic.CompareAndSwapInt64(&a.value, old, new)
}

func (a *AtomicBoolean) Set(val bool) {
	a.value = val
}


func (a *AtomicBoolean) Get() bool {
	return a.value
}

type VolatileBoolean struct {
	value *atomic.Value
}
func NewVolatileBoolean(val bool) *VolatileBoolean {
	value := new(atomic.Value)
	value.Store(val)
	return &AtomicBoolean{value:value}
}

func (b *VolatileBoolean) Get() bool {
	return (b.value.Load()).(bool)
}

func (b *VolatileBoolean) Set(val bool) {
	b.value.Store(val)
}
