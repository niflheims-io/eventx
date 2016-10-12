package eventx

import "errors"

type Error struct {
	e error
}

func NewError(e error) *Error {
	return &Error{e:e}
}

func (r *Error) Error() error {
	return r.e
}

type InsufficientCapacityError struct {
	*Error
}

func NewInsufficientCapacityError() *InsufficientCapacityError {
	return &InsufficientCapacityError{
		Error:NewError(errors.New("Insufficient capacity error.")),
	}
}

type AlertError struct {
	*Error
}

func NewAlertError() *AlertError {
	return &AlertError{
		Error:NewError(errors.New("Alert error.")),
	}
}

type InterruptedError struct {
	*Error
}

func NewInterruptedError() *InterruptedError {
	return &InterruptedError{
		Error:NewError(errors.New("Interrupted error.")),
	}
}

type TimeoutError struct {
	*Error
}

func NewTimeoutError() *TimeoutError {
	return &TimeoutError{
		Error:NewError(errors.New("Timeout error.")),
	}
}

// ExceptionHandler

// const error
