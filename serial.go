package eventx

import (
	"sync/atomic"
	"unsafe"
	"time"
)

type Indexed interface {
	GetIndex() int64
}

type Serialized interface {
	GetBufferSize() int
	HasAvailableCapacity(int) bool
	RemainingCapacity() int64
	Next() int64
	NextN(int) int64
	TryNext() (int64, *InsufficientCapacityError)
	TryNextN(int) (int64, *InsufficientCapacityError)
	Publish(int64)
	PublishRange(int64, int64)
}

type Serializer interface {
	Indexed
	Serialized
	Claim(int64)
	IsAvailable(int64)
	AddGatingSerial(...*Serial)
	RemoveGatingSerial(*Serial)
	NewBarrier(...*Serial) SerialBarrier
	GetMinimumSerial() int64
	GetHighestPublishedSerial(int64, int64) int64
	NewPoll(BufferProvider, ...*Serial) *EventPoll
}

const SERIAL_INITIAL_VALUE = int64(-1)

type Serial struct {
	lhsPadding []int64
	value int64
	rhsPadding []int64
}

func NewSerial() *Serial {
	return &Serial{
		lhsPadding:make([]int64, 7),
		value:SERIAL_INITIAL_VALUE,
		rhsPadding:make([]int64, 7),
	}
}

func (s *Serial) Get() int64 {
	return s.value
}

func (s *Serial) Set(n int64) {
	s.value = n
}

func (s *Serial) CompareAndSet(oldValue int64, newValue int64) bool {
	return atomic.CompareAndSwapInt64(&s.value, oldValue, newValue)
}

func (s *Serial) IncrementAndGet() int64 {
	return s.AddAndGet(int64(1))
}

func (s *Serial) AddAndGet(increment int64) int64 {
	var currentValue int64
	var newValue int64
	for {
		currentValue = s.Get()
		newValue = currentValue + increment
		if s.CompareAndSet(currentValue, newValue) {
			break
		}
	}
	return newValue
}

type abstractSerializer struct {
	bufferSize int
	waitStrategy WaitStrategy
	index *Serial
	gatingSerials []*Serial
}

func newAbstractSequencer(bufferSize int, w WaitStrategy) *abstractSerializer {
	if bufferSize < 1 {
		panic("bufferSize must not be less than 1")
	}
	if bufferSize > 0 && ( bufferSize & ( bufferSize - 1 ) ) != 0  {
		panic("The bufferSize must be a power of two, e.g. 2, 4, 8, 16, 32, 64, etc.")
	}
	s := abstractSerializer{
		bufferSize:bufferSize,
		waitStrategy:w,
		index:NewSerial(),
		gatingSerials:make([]*Serial, 0),
	}
	return &s
}

func (s *abstractSerializer) GetIndex() int64 {
	return s.index.Get()
}

//getBufferSize
func (s *abstractSerializer) GetBufferSize() int {
	return s.bufferSize
}

//addGatingSequences
func (s *abstractSerializer) AddGatingSerial(gatingSerials ...*Serial) {
	var indexSerials int64
	var updatedSerials []*Serial
	var currentSerials []*Serial
	for  {
		currentSerials = s.gatingSerials
		updatedSerials = make([]*Serial, len(currentSerials) + 1)
		copy(updatedSerials, currentSerials)
		indexSerials = s.GetIndex()
		gatingSequencesLen := len(gatingSerials)
		index := len(currentSerials)
		for i := 0 ; i < gatingSequencesLen; i ++ {
			gatingSerials[i].Set(indexSerials)
			updatedSerials[index] = gatingSerials[i]
			index ++
		}
		oldPoint := unsafe.Pointer(&s.gatingSerials)
		newPoint := unsafe.Pointer(&updatedSerials)
		swapOk := atomic.CompareAndSwapPointer(&oldPoint, oldPoint, newPoint)
		if swapOk {
			s.gatingSerials = *(*[]*Serial)(atomic.LoadPointer(&oldPoint))
			break
		}
	}
}

func (s *abstractSerializer) RemoveGatingSerial(serial *Serial) bool {
	numRemoved := 0
	var oldSerials []*Serial
	var newSerials []*Serial
	for {
		oldSerials = s.gatingSerials
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
			oldPoint := unsafe.Pointer(&s.gatingSerials)
			newPoint := unsafe.Pointer(&newSerials)
			swapOk := atomic.CompareAndSwapPointer(&oldPoint, oldPoint, newPoint)
			if swapOk {
				s.gatingSerials = *(*[]*Serial)(atomic.LoadPointer(&oldPoint))
				break
			}
		}
	}
	return numRemoved > 0
}

func (s *abstractSerializer) GetMinimumSerial() int64 {
	return getMinimumSerial(s.gatingSerials, s.index.Get())
}

func (s *abstractSerializer) NewBarrier(serialsToTrack ...*Serial) SerialBarrier {
	return NewProcessingSerialBarrier(s, s.waitStrategy, s.index, s.gatingSerials)
}

func (s *abstractSerializer) NewPoll(bufferProvider BufferProvider, gatingSerials ...*Serial) *EventPoll {
	return NewEventPoll(bufferProvider, s, NewSerial(), s.index, gatingSerials...)
}


type SingleProducerSerializer struct {
	*abstractSerializer
	lhsPadding []int64
	nextValue int64
	cachedValue int64
	rhsPadding []int64
}

func NewSingleProducerSequencer(bufferSize int, waitStrategy WaitStrategy) *SingleProducerSerializer {
	return &SingleProducerSerializer{
		abstractSerializer:newAbstractSequencer(bufferSize, waitStrategy),
		lhsPadding:make([]int64, 7),
		nextValue:int64(-1),
		cachedValue:int64(-1),
		rhsPadding:make([]int64, 7),
	}
}

func (s *SingleProducerSerializer) HasAvailableCapacity(requiredCapacity int) bool {
	nextValue := s.nextValue
	wrapPoint := (nextValue + int64(requiredCapacity)) - int64(s.bufferSize)
	cachedGatingSerial := s.cachedValue
	if wrapPoint > cachedGatingSerial || cachedGatingSerial > nextValue {
		minSerial := getMinimumSerial(s.gatingSerials, nextValue)
		s.cachedValue = minSerial
		if wrapPoint > minSerial {
			return false
		}
	}
	return true
}

//next
func (s *SingleProducerSerializer) Next() int64 {
	return s.NextN(1)
}

func (s *SingleProducerSerializer) NextN(n int) int64 {
	if n < 0 {
		panic("n must be > 0")
	}
	nextValue := s.nextValue
	nextSerial := nextValue + int64(n)
	wrapPoint := nextSerial - int64(s.bufferSize)
	cachedGatingSerial := s.cachedValue
	if wrapPoint > cachedGatingSerial || cachedGatingSerial > nextValue {
		minSerial := getMinimumSerial(s.gatingSerials, nextValue)
		for {
			if wrapPoint > minSerial {
				s.waitStrategy.SignalAllWhenBlocking()
				time.Sleep(time.Nanosecond * 1)
				minSerial = getMinimumSerial(s.gatingSerials, nextValue)
			} else {
				break
			}
		}
		s.cachedValue = minSerial
	}
	s.nextValue = nextSerial
	return nextSerial
}

//tryNext

func (s *SingleProducerSerializer) TryNext() (int64, error) {
	return s.TryNextN(1)
}


func (s *SingleProducerSerializer) TryNextN(n int) (int64, *InsufficientCapacityError) {
	if n < 0 {
		panic("n must be > 0")
	}
	if !s.HasAvailableCapacity(n) {
		return int64(0), NewInsufficientCapacityError()
	}
	s.nextValue = s.nextValue + int64(n)
	nextSerial := s.nextValue
	return nextSerial, nil
}

//remainingCapacity
func (s *SingleProducerSerializer) RemainingCapacity() int64 {
	nextValue := s.nextValue
	consumed := getMinimumSerial(s.gatingSerials, nextValue)
	produced := nextValue
	return int64(s.GetBufferSize()) - (produced - consumed)
}

//claim
func (s *SingleProducerSerializer) Claim(serialNo int64) {
	s.nextValue = serialNo
}

//publish
func (s *SingleProducerSerializer) Publish(serialNo int64) {
	s.index.Set(serialNo)
	s.waitStrategy.SignalAllWhenBlocking()
}

//PublishRange(int64, int64)
func (s *SingleProducerSerializer) PublishRange(lowSerialNo int64, highSerialNo int64) {
	s.Publish(highSerialNo)
}

//isAvailable
func (s *SingleProducerSerializer) IsAvailable(serialNo int64) bool {
	return serialNo <= s.index.Get()
}

//GetHighestPublishedSequence(int64, int64) int64
func (s *SingleProducerSerializer) GetHighestPublishedSerial(lowerBound int64, availableSerialNo int64) int64 {
	return availableSerialNo
}






