package driver

type Batch interface {
	UpToStage(v ...interface{}) error
	CopyInto() error
}
