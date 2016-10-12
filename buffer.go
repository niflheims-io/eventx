package eventx


// interface
type BufferProvider interface {
	Get(int64) interface{}
}