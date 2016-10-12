package eventx

import (
	"unsafe"
	"sync/atomic"
	"math"
)

func getMinimumSerialFromMin(serials []*Serial) int64 {
	return getMinimumSerial(serials, math.MaxInt64)
}

func getMinimumSerial(serials []*Serial, min int64) int64 {
	n := len(serials)
	for i := 0 ; i < n ; i++  {
		value := serials[i].Get()
		if value < min {
			min = value
		}
	}
	return min
}

//SerialSet
type SerialSet struct {
	*Serial
	serials []*Serial
}

func NewSerialSet() *SerialSet {
	return &SerialSet{
		Serial:NewSerial(),
		serials:make([]*Serial, 0),
	}
}

func (s *SerialSet) GetSerials() []*Serial {
	return s.serials
}

func (s *SerialSet) Get() int64 {
	return getMinimumSerialFromMin(s.serials)
}

func (s *SerialSet) Set(value int64) {
	serials := s.serials
	size := len(serials)
	for i := 0 ; i < size ; i ++ {
		serials[i].Set(value)
	}
}

func (s *SerialSet) Add(serial *Serial) {
	var oldSerials []*Serial
	var newSerials []*Serial
	for {
		oldSerials = s.serials
		oldSize := len(oldSerials)
		newSerials = make([]*Serial, oldSize + 1)
		copy(newSerials, oldSerials)
		newSerials[oldSize] = serial
		oldPoint := unsafe.Pointer(&s.serials)
		newPoint := unsafe.Pointer(&newSerials)
		swapOk := atomic.CompareAndSwapPointer(&oldPoint, oldPoint, newPoint)
		if swapOk {
			s.serials = *(*[]*Serial)(atomic.LoadPointer(&oldPoint))
			break
		}
	}
}

func (s *SerialSet) Remove(serial *Serial) bool {
	numRemoved := 0
	var oldSerials []*Serial
	var newSerials []*Serial
	for {
		oldSerials = s.serials
		oldSize := len(oldSerials)
		tempSequences := make([]*Serial, 0, oldSize)
		for i := 0 ; i < oldSize ; i++  {
			if oldSerials[i].Get() == serial.Get() {
				numRemoved ++
				continue
			}
			tempSequences = append(tempSequences, oldSerials[i])
		}
		if numRemoved > 0 {
			newSerials = make([]*Serial, len(tempSequences))
			copy(newSerials, tempSequences)
			oldPoint := unsafe.Pointer(&s.serials)
			newPoint := unsafe.Pointer(&newSerials)
			swapOk := atomic.CompareAndSwapPointer(&oldPoint, oldPoint, newPoint)
			if swapOk {
				s.serials = *(*[]*Serial)(atomic.LoadPointer(&oldPoint))
				break
			}
		}
	}
	return numRemoved > 0
}

func (s *SerialSet) Size() int {
	return len(s.serials)
}

func (s *SerialSet) AddWhileRunning(indexed Indexed, serial *Serial)  {
	var indexSerial int64
	var updatedSerials []*Serial
	var currentSerials []*Serial
	for  {
		currentSerials = s.serials
		updatedSerials = make([]*Serial, len(currentSerials) + 1)
		copy(updatedSerials, currentSerials)
		indexSerial = indexed.GetIndex()
		index := len(currentSerials)
		serial.Set(indexSerial)
		updatedSerials[index] = serial
		oldPoint := unsafe.Pointer(&s.serials)
		newPoint := unsafe.Pointer(&updatedSerials)
		swapOk := atomic.CompareAndSwapPointer(&oldPoint, oldPoint, newPoint)
		if swapOk {
			s.serials = *(*[]*Serial)(atomic.LoadPointer(&oldPoint))
			break
		}
	}
}

type FixedSerialSet struct {
	*Serial
	serials []*Serial

}

func NewFixedSerialSet(serials []*Serial) *FixedSerialSet {
	newSerials := make([]*Serial, len(serials))
	copy(newSerials, serials)
	return &FixedSerialSet{
		Serial:NewSerial(),
		serials:newSerials,
	}
}

func (s *FixedSerialSet) Get() int64 {
	return getMinimumSerialFromMin(s.serials)
}