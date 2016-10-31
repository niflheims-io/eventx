package eventx

import "runtime"

// EventProcessor
type EventProcessor interface {
	Routine
	GetSerial() *Serial
	Halt()
	IsRunning() bool
	SetErrorHandler(ErrorHandler)
	SetTimeoutHandler(TimeoutHandler)
}

// BatchEventProcessor
type BatchEventProcessor struct {
	running *AtomicBoolean
	errorHandler ErrorHandler
	timeoutHandler TimeoutHandler
	bufferProvider BufferProvider
	serialBarrier SerialBarrier
	eventHandler EventHandler
	serial *Serial

}

func NewBatchEventProcessor(bufferProvider BufferProvider, serialBarrier SerialBarrier, eventHandler EventHandler) *BatchEventProcessor {
	p := BatchEventProcessor{}
	p.bufferProvider = bufferProvider
	p.serialBarrier = serialBarrier
	p.eventHandler = eventHandler
	p.running = NewAtomicBoolean(false)
	p.serial = NewSerial()
	return &p
}

func (p *BatchEventProcessor) GetSerial() *Serial {
	return p.serial
}

func (p *BatchEventProcessor) Halt() {
	p.running.Set(false)
	p.serialBarrier.Alert()
}


func (p *BatchEventProcessor) IsRunning() bool {
	return p.running.Get()
}

func (p *BatchEventProcessor) SetErrorHandler(errorHandler ErrorHandler) {
	p.errorHandler = errorHandler
}


func (p *BatchEventProcessor) SetTimeoutHandler(timeoutHandler TimeoutHandler) {
	p.timeoutHandler = timeoutHandler
}

//run
func (p *BatchEventProcessor) Run() {
	defer p.running.Set(false)
	if !p.running.CompareAndSet(false, true) {
		return
	}
	p.serialBarrier.ClearAlert()
	var event interface{}
	nextSerial := p.serial.Get() + int64(1)
	for {
		availableSerial, waitErr := p.serialBarrier.WaitFor(nextSerial)
		if waitErr != nil {
			if waitErr.Error() == AlertError.Error() {
				if !p.running.Get() {
					break
				}
			} else if waitErr.Error() == TimeoutError.Error() {
				p.notifyTimeout(p.serial.Get())
			} else {
				p.errorHandler.HandleEventError(waitErr, nextSerial, event)
				p.serial.Set(nextSerial)
				nextSerial++
			}
		}
		for {
			if nextSerial <= availableSerial {
				event = p.bufferProvider.Get(nextSerial)
				p.eventHandler.OnEvent(event, nextSerial, nextSerial == availableSerial)
				nextSerial++
			} else {
				break
			}
		}
		p.serial.Set(availableSerial)
	}
}

func (p *BatchEventProcessor) notifyTimeout(availableSerial int64) {
	if p.timeoutHandler != nil {
		err := p.timeoutHandler.OnTimeout(availableSerial)
		if err != nil {
			if p.errorHandler != nil {
				p.errorHandler.HandleEventError(err, availableSerial, nil)
			}
		}
	}
}


// MultiBufferBatchEventProcessor
type MultiBufferBatchEventProcessor struct {
	running *AtomicBoolean
	providers []BufferProvider
	barriers []SerialBarrier
	eventHandler EventHandler
	errorHandler ErrorHandler
	timeoutHandler TimeoutHandler
	serials []*Serial
	count int64
}

func NewMultiBufferBatchEventProcessor(eventHandler EventHandler) *MultiBufferBatchEventProcessor {
	p := MultiBufferBatchEventProcessor{}
	p.eventHandler = eventHandler
	p.running = NewAtomicBoolean(false)
	p.providers = make([]BufferProvider, 0, 1)
	p.barriers = make([]SerialBarrier, 0, 1)
	p.serials = make([]*Serial, 0, 1)
	return &p
}

func (p *MultiBufferBatchEventProcessor) IsRunning() bool {
	return p.running.Get()
}

func (p *MultiBufferBatchEventProcessor) SetErrorHandler(errorHandler ErrorHandler) {
	p.errorHandler = errorHandler
}


func (p *MultiBufferBatchEventProcessor) SetTimeoutHandler(timeoutHandler TimeoutHandler) {
	p.timeoutHandler = timeoutHandler
}

func (p *MultiBufferBatchEventProcessor) AddProviderAndBarrier(provider BufferProvider, barrier SerialBarrier) {
	if provider == nil || barrier == nil {
		return
	}
	if p.IsRunning() {
		panic("can not add provider and barrier when running.")
	}
	p.providers = append(p.providers, provider)
	p.barriers = append(p.barriers, barrier)
	p.serials = append(p.serials, NewSerial())
}


func (p *MultiBufferBatchEventProcessor) Halt() {
	p.running.Set(false)
	p.barriers[0].Alert()
}

func (p *MultiBufferBatchEventProcessor) GetCount() int64 {
	return p.count
}

func (p *MultiBufferBatchEventProcessor) Run() {
	defer p.running.Set(false)
	if !p.running.CompareAndSet(false, true) {
		return
	}
	barriersLen := len(p.barriers)
	if barriersLen == 0 {
		panic("No producers.")
	}
	for i := 0 ; i < barriersLen ; i ++ {
		p.barriers[i].ClearAlert()
	}
	for {
		for i := 0 ; i < barriersLen ; i ++ {
			var event interface{}
			nextSerial := p.serials[i].Get() + int64(1)
			available, waitErr := p.barriers[i].WaitFor(int64(-1))
			if waitErr != nil {
				if waitErr.Error() == AlertError.Error() {
					if !p.running.Get() {
						break
					}
				} else if waitErr.Error() == TimeoutError.Error() {
					p.notifyTimeout(p.serials[i].Get())
				} else {
					p.errorHandler.HandleEventError(waitErr, nextSerial, event)
					p.serials[i].Set(nextSerial)
					nextSerial++
				}
			}
			for j := nextSerial ; j <= available ; j ++ {
				p.eventHandler.OnEvent(p.providers[i].Get(j), j, nextSerial == available)
			}
			p.serials[i].Set(available)
			p.count = p.count + (available - nextSerial + int64(1))
		}
		runtime.Gosched()
	}
}

func (p *MultiBufferBatchEventProcessor) notifyTimeout(availableSerial int64) {
	if p.timeoutHandler != nil {
		err := p.timeoutHandler.OnTimeout(availableSerial)
		if err != nil {
			if p.errorHandler != nil {
				p.errorHandler.HandleEventError(err, availableSerial, nil)
			}
		}
	}
}