package eventx

// Routine
type Routine interface {
	Run()
}

type RoutineExecutor interface {
	Execute(Routine)
}

// BasicExecutor
type BasicRoutineExecutor struct {

}

func NewBasicRoutineExecutor() *BasicRoutineExecutor {
	return &BasicRoutineExecutor{}
}

func (e *BasicRoutineExecutor) Execute(r Routine)  {
	r.Run()
}



