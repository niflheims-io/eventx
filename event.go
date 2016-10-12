package eventx


//EventFactory
type EventFactory interface {
	NewEvent() interface{}
}
//EventHandler
type EventHandler interface {
	OnEvent(interface{}, int64, bool) error
}

// EventPoll

type EventPoll struct {
	bufferProvider BufferProvider
	serializer Serializer
	serial *Serial
	gatingSerial *Serial
}

const (
	POLL_STATE_PROCESSING = "PROCESSING"
	POLL_STATE_GATING = "GATING"
	POLL_STATE_IDLE = "IDLE"
)

type EventPollHandler interface {
	OnEvent(interface{}, int64, bool) (bool, error)
}

func NewEventPoll(bufferProvider BufferProvider, serializer Serializer, serial *Serial, indexSerial *Serial, gatingSerials ...*Serial) *EventPoll {
	var gatingSerial *Serial = nil
	gatingSerialsLen := len(gatingSerial)
	if gatingSerialsLen == 0 {
		gatingSerial = indexSerial
	} else if gatingSerialsLen == 1 {
		gatingSerial = gatingSerials[0]
	} else {
		gatingSerial = NewFixedSerialSet(gatingSerials)
	}
	return &EventPoll{
		bufferProvider:bufferProvider,
		serializer:serializer,
		serial:serial,
		gatingSerial:gatingSerial,
	}
}

func (e *EventPoll) Poll(eventHandler EventPollHandler) (string, error) {
	currentSerial := e.serial.Get()
	nextSerial := currentSerial + int64(1)
	availableSerial := e.serializer.GetHighestPublishedSerial(nextSerial, e.gatingSerial.Get())
	if nextSerial <= availableSerial {
		processNextEvent := false
		var processNextEventErr error
		processedSerial := currentSerial
		defer e.serial.Set(processedSerial)
		for {
			event := e.bufferProvider.Get(nextSerial)
			processNextEvent, processNextEventErr = eventHandler.OnEvent(event, nextSerial, nextSerial == availableSerial)
			if processNextEventErr != nil {
				return "", processNextEventErr
			}
			nextSerial++
			if (nextSerial <= availableSerial & processNextEvent) == false {
				break
			}
		}
		return POLL_STATE_PROCESSING, nil
	} else if e.serializer.GetIndex() >= nextSerial {
		return POLL_STATE_GATING, nil
	} else {
		return POLL_STATE_IDLE, nil
	}
}