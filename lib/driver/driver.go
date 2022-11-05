package driver

type Batch interface {
	AppendToFile(v ...interface{}) error
	UploadToStage() error
	CopyInto() error
}
