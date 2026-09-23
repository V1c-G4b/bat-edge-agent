package collector

import (
	"fmt"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
)

type SystemCollector struct{}

func NewSystemCollector() *SystemCollector {
	return &SystemCollector{}
}

func (m SystemMetrics) String() string {
	return fmt.Sprintf(
		"CPU: %.2f%% | Memory: %.2f%% (%s / %s) | Collected at: %s",
		m.CPUUsagePercentual,
		m.MemUsagePercentual,
		formatBytes(m.MemUsedBytes),
		formatBytes(m.MemTotalBytes),
		time.Unix(m.TimeStamp, 0).Format("15:04:05 02/01/2006"),
	)
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func (s *SystemCollector) Collect() (SystemMetrics, error) {

	cpuPercent, err := cpu.Percent(200*time.Millisecond, false)
	if err != nil {
		return SystemMetrics{}, err
	}

	vMem, err := mem.VirtualMemory()
	if err != nil {
		return SystemMetrics{}, err
	}

	var cpuUsage float64
	if len(cpuPercent) > 0 {
		cpuUsage = cpuPercent[0]
	}

	return SystemMetrics{
		CPUUsagePercentual: cpuUsage,
		MemUsedBytes:       int64(vMem.Used),
		MemTotalBytes:      int64(vMem.Total),
		MemUsagePercentual: vMem.UsedPercent,
		TimeStamp:          time.Now().Unix(),
	}, nil
}
