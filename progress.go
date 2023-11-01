package godatabend

type ProgressInfo struct {
	ProcessedRows  uint64 `json:"processed_rows"`
	TotalRows      uint64 `json:"total_rows"`
	ProcessedBytes uint64 `json:"processed_bytes"`
	TotalBytes     uint64 `json:"total_bytes"`
}

type ProgressTracker func(*ProgressInfo)
