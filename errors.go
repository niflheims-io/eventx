package eventx

import (
	"errors"
	"log"
)

var (
	InsufficientCapacityError = errors.New("Insufficient capacity error.")
	AlertError = errors.New("Alert error.")
	InterruptedError = errors.New("Interrupted error.")
	TimeoutError = errors.New("Timeout error.")
)

type ErrorHandler interface {
	HandleEventError(error, int64, interface{})
	HandleOnStartError(error)
	HandleOnShutdownError(error)
}

type FatalErrorHandler struct {
	logger *log.Logger
}

func NewFatalErrorHandler(logger *log.Logger) *FatalErrorHandler {
	return &FatalErrorHandler{logger:logger}
}

func (h *FatalErrorHandler) HandleEventError(err error, sequence int64, event interface{}) {
	h.logger.Fatalln("Error processing: ", sequence, event, err)
}

func (h *FatalErrorHandler) HandleOnStartError(err error) {
	h.logger.Fatalln("Exception during OnStart()", err)
}

func (h *FatalErrorHandler) HandleOnShutdownError(err error) {
	h.logger.Fatalln("Exception during OnShutdown()", err)
}

type TimeoutHandler interface {
	OnTimeout(int64) error
}

