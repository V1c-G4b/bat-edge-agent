package collector

type Collector interface {
	Collect() (SystemMetrics, error)
}

type SystemMetrics struct {
	TimeStamp          int64
	CPUUsagePercentual float64
	MemTotalBytes      int64
	MemUsedBytes       int64
	MemUsagePercentual float64
}
