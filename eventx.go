package eventx

import (
	"errors"
	"sync"
)

type Producer struct {
	*RingBuf
}

type EventX struct {
	producer *Producer
	batchProcessor *BatchEventProcessor
}


func NewEventX(eventFactory EventFactory, bufSize int, waitStrategy WaitStrategy, eventHandler EventHandler) *EventX {
	serializer := NewSingleProducerSerializer(bufSize, waitStrategy)
	producer := &Producer{RingBuf:NewRingBuf(eventFactory, bufSize, serializer)}
	barrier := producer.NewBarrier()
	batchProcessor := NewBatchEventProcessor(producer, barrier, eventHandler)
	return &EventX{producer:producer, batchProcessor:batchProcessor}
}

func (e *EventX) GetProducer() *Producer {
	return e.producer
}

func (e *EventX) GetEventProcessor() *BatchEventProcessor {
	return e.batchProcessor
}

func (e *EventX) Run() {
	e.producer.AddGatingSerials(e.batchProcessor.GetSerial())
	go e.batchProcessor.Run()
}

type EventMultiX struct {
	lock *sync.Mutex
	producers map[string]*Producer
	batchProcessor *MultiBufferBatchEventProcessor
	eventFactory EventFactory
	bufSize int
	waitStrategy WaitStrategy
}

func NewEventMultiX(eventFactory EventFactory, bufSize int, waitStrategy WaitStrategy, eventHandler EventHandler) *EventMultiX {
	producers := make(map[string]*Producer)
	batchProcessor := NewMultiBufferBatchEventProcessor(eventHandler)
	return &EventMultiX{lock:new(sync.Mutex), producers:producers, batchProcessor:batchProcessor, eventFactory:eventFactory, bufSize:bufSize, waitStrategy:waitStrategy}
}

func (e *EventMultiX) NewProducer(name string) (*Producer, error) {
	e.lock.Lock()
	defer e.lock.Unlock()
	if e.batchProcessor.IsRunning() {
		return nil, errors.New("EventBatchProcessor is running, can not add producer.")
	}
	if producer, ok := e.producers[name]; ok {
		return producer, nil
	}
	serializer := NewSingleProducerSerializer(e.bufSize, e.waitStrategy)
	producer := &Producer{RingBuf:NewRingBuf(e.eventFactory, e.bufSize, serializer)}
	barrier := producer.NewBarrier()
	e.batchProcessor.AddProviderAndBarrier(producer, barrier)
	e.producers[name] = producer
	return producer, nil
}

func (e *EventMultiX) Run() {
	go e.batchProcessor.Run()
}