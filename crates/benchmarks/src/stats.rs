//! Statistics utilities for benchmark timing analysis

use serde::{Deserialize, Serialize};

/// Timing statistics for compression or decompression operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimingStats {
    /// Average time in milliseconds
    pub avg_ms: f64,
    /// Standard deviation of time in milliseconds
    pub std_ms: f64,
    /// Average throughput in MB/s
    pub throughput_mbps: f64,
    /// Standard deviation of throughput in MB/s
    pub throughput_std_mbps: f64,
}

impl TimingStats {
    /// Compute timing statistics from iteration times and total bytes processed
    ///
    /// # Arguments
    /// * `times_ms` - Total time in milliseconds for each iteration
    /// * `total_bytes` - Total bytes processed per iteration
    pub fn from_times(times_ms: &[f64], total_bytes: usize) -> Self {
        let avg_ms = mean(times_ms);
        let std_ms = std_dev(times_ms, avg_ms);

        // Compute throughput for each iteration
        let throughputs: Vec<f64> = times_ms
            .iter()
            .map(|&t| {
                if t > 0.0 {
                    (total_bytes as f64 / 1_000_000.0) / (t / 1000.0)
                } else {
                    0.0
                }
            })
            .collect();

        let throughput_mbps = mean(&throughputs);
        let throughput_std_mbps = std_dev(&throughputs, throughput_mbps);

        Self {
            avg_ms,
            std_ms,
            throughput_mbps,
            throughput_std_mbps,
        }
    }
}

/// Compute mean of values
pub fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f64>() / values.len() as f64
}

/// Compute sample standard deviation
pub fn std_dev(values: &[f64], mean: f64) -> f64 {
    if values.len() <= 1 {
        return 0.0;
    }
    let variance = values.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
    variance.sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mean() {
        assert_eq!(mean(&[1.0, 2.0, 3.0, 4.0, 5.0]), 3.0);
        assert_eq!(mean(&[]), 0.0);
        assert_eq!(mean(&[42.0]), 42.0);
    }

    #[test]
    fn test_std_dev() {
        // Known values: [2, 4, 4, 4, 5, 5, 7, 9] has std dev of 2.0
        let values = vec![2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let m = mean(&values);
        let std = std_dev(&values, m);
        assert!((std - 2.138).abs() < 0.01); // Sample std dev

        // Single value should have 0 std dev
        assert_eq!(std_dev(&[42.0], 42.0), 0.0);

        // Empty should have 0 std dev
        assert_eq!(std_dev(&[], 0.0), 0.0);
    }

    #[test]
    fn test_timing_stats() {
        let times = vec![100.0, 100.0, 100.0]; // 100ms each
        let total_bytes = 10_000_000; // 10 MB

        let stats = TimingStats::from_times(&times, total_bytes);

        assert_eq!(stats.avg_ms, 100.0);
        assert_eq!(stats.std_ms, 0.0);
        // 10 MB in 0.1 seconds = 100 MB/s
        assert!((stats.throughput_mbps - 100.0).abs() < 0.01);
        assert_eq!(stats.throughput_std_mbps, 0.0);
    }
}
