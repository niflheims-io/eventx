package eventx


// serialBarrier -> SequenceBarrier

type SerialBarrier interface {
	WaitFor(int64) (int64, *AlertError, *InterruptedError, *TimeoutError)
	GetIndex() int64
	IsAlerted() bool
	Alert()
	ClearAlert()
	CheckAlert() *AlertError
}

type ProcessingSerialBarrier struct {
	waitStrategy WaitStrategy
	dependentSerial *Serial
	alerted *VolatileBoolean
	indexSerial *Serial
	serializer Serializer
}

func NewProcessingSerialBarrier(serializer Serializer, waitStrategy WaitStrategy, indexSerial *Serial, dependentSerials []*Serial) *ProcessingSerialBarrier  {
	var dependentSerial *Serial
	if len(dependentSerials) == 0 {
		dependentSerial = indexSerial
	} else {
		dependentSerial = NewFixedSerialSet(dependentSerials)
	}
	return &ProcessingSerialBarrier{
		serializer:serializer,
		waitStrategy:waitStrategy,
		indexSerial:indexSerial,
		alerted:NewVolatileBoolean(false),
		dependentSerial:dependentSerial,
	}
}

//waitFor
func (b *ProcessingSerialBarrier) WaitFor(serial int64) (int64, *AlertError, *InterruptedError, *TimeoutError) {
	checkErr := b.CheckAlert()
	if checkErr != nil {
		return int64(0), checkErr, nil, nil
	}
	availableSerial, waitErr := b.waitStrategy.WaitFor(serial, b.indexSerial, b.dependentSerial, b)
	if waitErr != nil {
		return int64(0), waitErr
	}
	if availableSerial < serial {
		return availableSerial, nil
	}
	return b.serializer.GetHighestPublishedSerial(serial, availableSerial), nil
}

//clearAlert
func (b *ProcessingSerialBarrier) ClearAlert() {
	b.alerted.Set(false)
}

//checkAlert
func (b *ProcessingSerialBarrier) CheckAlert() *AlertError {
	if b.alerted.Get() {
		return NewAlertError()
	}
	return nil
}

//alert
func (b *ProcessingSerialBarrier) Alert() {
	b.alerted.Set(true)
	b.waitStrategy.SignalAllWhenBlocking()
}

//isAlerted
func (b *ProcessingSerialBarrier) IsAlerted() bool {
	return b.alerted.Get()
}

//getCursor
func (b *ProcessingSerialBarrier) GetIndex() int64 {
	return b.dependentSerial.Get()
}