package eventx

import "unsafe"

// ring buffer

type RingBuf struct {
	lhsPadding []int64
	bufferPad int64
	indexMask int64
	entries []interface{}
	bufferSize int
	serializer Serializer
	rhsPadding []int64
}

func NewRingBuf(eventFactory EventFactory, bufferSize int, serializer Serializer) *RingBuf {
	indexMask := int64(bufferSize - 1)
	var entries []interface{}
	scale := unsafe.Alignof(&entries)
	bufferPad := 128 / scale
	entries = make([]interface{}, bufferSize + 2 * (int)(bufferPad))
	bufferPadInt := int(bufferPad)
	for i := 0; i < bufferSize; i++ {
		e := eventFactory.NewEvent()
		entries[i + bufferPadInt] = e
	}
	rb := RingBuf{
		lhsPadding:make([]int64, 7),
		bufferPad:int64(bufferPadInt),
		indexMask:indexMask,
		entries:entries,
		bufferSize:bufferSize,
		serializer:serializer,
		rhsPadding:make([]int64, 7),
	}
	return &rb
}

func (rb *RingBuf) elementAt(serialNo int64) interface{} {
	idx := serialNo & rb.indexMask + rb.bufferPad
	return rb.entries[idx]
}

func (rb *RingBuf) Get(serialNo int64) interface{} {
	return rb.elementAt(serialNo)
}

func (rb *RingBuf) Next() int64 {
	return rb.serializer.Next()
}

func (rb *RingBuf) NextN(n int) int64 {
	return rb.serializer.NextN(n)
}

func (rb *RingBuf) TryNext() (int64, error) {
	return rb.serializer.TryNext()
}

func (rb *RingBuf) TryNextN(n int) (int64, error) {
	return rb.serializer.TryNextN(n)
}

func (rb *RingBuf) ResetTo(serialNo int64) {
	rb.serializer.Claim(serialNo)
	rb.serializer.Publish(serialNo)
}

func (rb *RingBuf) ClaimAndGetPreAllocated(serialNo int64) interface{} {
	rb.serializer.Claim(serialNo)
	return rb.Get(serialNo)
}

func (rb *RingBuf) IsPublished(serial int64) bool {
	return rb.serializer.IsAvailable(serial)
}

func (rb *RingBuf) AddGatingSerials(gatingSerials ...*Serial) {
	rb.serializer.AddGatingSerials(gatingSerials...)
}

func (rb *RingBuf) GetMinimumGatingSerial() int64 {
	return rb.serializer.GetMinimumSerial()
}

func (rb *RingBuf) RemoveGatingSerial(serial *Serial) bool {
	return rb.serializer.RemoveGatingSerial(serial)
}

func (rb *RingBuf) NewBarrier(serialsToTrack ...*Serial) SerialBarrier {
	return rb.serializer.NewBarrier(serialsToTrack...)
}

/*
func (rb *RingBuf) NewPoll(gatingSerials ...*Serial) *EventPoll {
	return rb.serializer.NewPoll(gatingSerials...)
}
*/

func (rb *RingBuf) GetIndex() int64 {
	return rb.serializer.GetIndex()
}

func (rb *RingBuf) GetBufferSize() int {
	return rb.bufferSize
}

func (rb *RingBuf) HasAvailableCapacity(requiredCapacity int) bool {
	return rb.serializer.HasAvailableCapacity(requiredCapacity)
}

func (rb *RingBuf) Publish(sequence int64) {
	rb.serializer.Publish(sequence)
}

func (rb *RingBuf) PublishRange(low int64, high int64) {
	rb.serializer.PublishRange(low, high)
}

func (rb *RingBuf) RemainingCapacity() int64 {
	return rb.serializer.RemainingCapacity()
}
