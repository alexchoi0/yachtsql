use std::rc::Rc;
use std::time::Instant;

use super::memory_manager::{MemoryManager, MemoryManagerStatsSnapshot, MemoryPressure};
use super::memory_pool::{MemoryPool, PoolStats, TieredMemoryPool, TieredPoolStats};
use super::query_registry::{QueryRegistry, RegistryStats};

#[derive(Debug, Clone)]
pub struct ResourceReport {
    pub timestamp: Instant,
    pub pool_stats: Option<PoolStats>,
    pub tiered_pool_stats: Option<TieredPoolStats>,
    pub registry_stats: Option<RegistryStats>,
    pub manager_stats: Option<MemoryManagerStatsSnapshot>,
    pub health: SystemHealth,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemHealth {
    Healthy,
    Warning,
    Critical,
}

impl SystemHealth {
    pub fn as_str(&self) -> &'static str {
        match self {
            SystemHealth::Healthy => "healthy",
            SystemHealth::Warning => "warning",
            SystemHealth::Critical => "critical",
        }
    }
}

impl ResourceReport {
    pub fn new() -> Self {
        Self {
            timestamp: Instant::now(),
            pool_stats: None,
            tiered_pool_stats: None,
            registry_stats: None,
            manager_stats: None,
            health: SystemHealth::Healthy,
        }
    }

    pub fn with_pool_stats(mut self, pool: &Rc<MemoryPool>) -> Self {
        self.pool_stats = Some(pool.stats());
        self
    }

    pub fn with_tiered_pool_stats(mut self, pool: &Rc<TieredMemoryPool>) -> Self {
        self.tiered_pool_stats = Some(pool.stats());
        self
    }

    pub fn with_registry_stats(mut self, registry: &Rc<QueryRegistry>) -> Self {
        self.registry_stats = Some(registry.stats());
        self
    }

    pub fn with_manager_stats(mut self, manager: &MemoryManager) -> Self {
        self.manager_stats = Some(manager.stats());
        self
    }

    pub fn calculate_health(&mut self) {
        let mut health = SystemHealth::Healthy;

        if let Some(manager_stats) = &self.manager_stats {
            match manager_stats.current_pressure {
                MemoryPressure::Critical => health = SystemHealth::Critical,
                MemoryPressure::High if health == SystemHealth::Healthy => {
                    health = SystemHealth::Warning
                }
                _ => {}
            }
        }

        if let Some(pool_stats) = &self.pool_stats {
            if pool_stats.utilization > 0.95 && health == SystemHealth::Healthy {
                health = SystemHealth::Warning;
            }
        }

        if let Some(registry_stats) = &self.registry_stats {
            let failure_rate = if registry_stats.total_queries > 0 {
                registry_stats.failed as f64 / registry_stats.total_queries as f64
            } else {
                0.0
            };

            if failure_rate > 0.5 {
                health = SystemHealth::Critical;
            } else if failure_rate > 0.2 && health == SystemHealth::Healthy {
                health = SystemHealth::Warning;
            }
        }

        self.health = health;
    }

    pub fn format(&self) -> String {
        let mut report = String::new();

        report.push_str("=== RESOURCE USAGE REPORT ===\n\n");
        report.push_str(&format!(
            "System Health: {}\n\n",
            self.health.as_str().to_uppercase()
        ));

        if let Some(stats) = &self.pool_stats {
            report.push_str("--- Memory Pool ---\n");
            report.push_str(&format!(
                "  Budget: {} MB\n",
                stats.total_budget / (1024 * 1024)
            ));
            report.push_str(&format!(
                "  Allocated: {} MB ({:.1}%)\n",
                stats.allocated / (1024 * 1024),
                stats.utilization * 100.0
            ));
            report.push_str(&format!(
                "  Peak: {} MB\n",
                stats.peak_allocated / (1024 * 1024)
            ));
            report.push_str(&format!(
                "  Allocations: {} total, {} failed\n",
                stats.total_allocations, stats.failed_allocations
            ));
            report.push_str("\n");
        }

        if let Some(stats) = &self.tiered_pool_stats {
            report.push_str("--- Tiered Memory Pool ---\n");
            report.push_str(&format!(
                "  Total Budget: {} MB\n",
                stats.total_budget / (1024 * 1024)
            ));
            report.push_str(&format!(
                "  Global: {} / {} MB (peak: {} MB)\n",
                stats.global_allocated / (1024 * 1024),
                stats.total_budget / (1024 * 1024),
                stats.global_peak / (1024 * 1024)
            ));
            report.push_str("  By Priority:\n");
            report.push_str(&format!(
                "    Critical: {} MB (peak: {} MB)\n",
                stats.critical_allocated / (1024 * 1024),
                stats.critical_peak / (1024 * 1024)
            ));
            report.push_str(&format!(
                "    High: {} MB (peak: {} MB)\n",
                stats.high_allocated / (1024 * 1024),
                stats.high_peak / (1024 * 1024)
            ));
            report.push_str(&format!(
                "    Normal: {} MB (peak: {} MB)\n",
                stats.normal_allocated / (1024 * 1024),
                stats.normal_peak / (1024 * 1024)
            ));
            report.push_str(&format!(
                "    Low: {} MB (peak: {} MB)\n",
                stats.low_allocated / (1024 * 1024),
                stats.low_peak / (1024 * 1024)
            ));
            report.push_str("\n");
        }

        if let Some(stats) = &self.registry_stats {
            report.push_str("--- Query Registry ---\n");
            report.push_str(&format!("  Total Queries: {}\n", stats.total_queries));
            report.push_str(&format!("  Running: {}\n", stats.running));
            report.push_str(&format!("  Waiting: {}\n", stats.waiting));
            report.push_str(&format!("  Completed: {}\n", stats.completed));
            report.push_str(&format!("  Failed: {}\n", stats.failed));
            report.push_str(&format!("  Cancelled: {}\n", stats.cancelled));
            report.push_str(&format!(
                "  Active Memory: {} MB\n",
                stats.total_memory_bytes / (1024 * 1024)
            ));
            report.push_str("\n");
        }

        if let Some(stats) = &self.manager_stats {
            report.push_str("--- Memory Manager ---\n");
            report.push_str(&format!(
                "  Current Pressure: {}\n",
                stats.current_pressure.as_str()
            ));
            report.push_str(&format!(
                "  Peak Pressure: {}\n",
                stats.peak_pressure.as_str()
            ));
            report.push_str(&format!("  Queries Queued: {}\n", stats.queries_queued));
            report.push_str(&format!(
                "  Queries Cancelled: {}\n",
                stats.queries_cancelled
            ));
            report.push_str("\n");
        }

        report
    }

    pub fn format_compact(&self) -> String {
        let mut parts = Vec::new();

        parts.push(format!("Health: {}", self.health.as_str()));

        if let Some(stats) = &self.pool_stats {
            parts.push(format!("Memory: {:.1}%", stats.utilization * 100.0));
        }

        if let Some(stats) = &self.registry_stats {
            parts.push(format!("Queries: {}", stats.total_queries));
            parts.push(format!("Running: {}", stats.running));
        }

        if let Some(stats) = &self.manager_stats {
            parts.push(format!("Pressure: {}", stats.current_pressure.as_str()));
        }

        parts.join(" | ")
    }
}

impl Default for ResourceReport {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ResourceMonitor {
    pool: Option<Rc<MemoryPool>>,
    tiered_pool: Option<Rc<TieredMemoryPool>>,
    registry: Option<Rc<QueryRegistry>>,
    manager: Option<Rc<MemoryManager>>,
    history: std::rc::Rc<std::cell::RefCell<Vec<ResourceReport>>>,
    max_history: usize,
}

impl ResourceMonitor {
    pub fn new(max_history: usize) -> Self {
        Self {
            pool: None,
            tiered_pool: None,
            registry: None,
            manager: None,
            history: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
            max_history,
        }
    }

    pub fn with_pool(mut self, pool: Rc<MemoryPool>) -> Self {
        self.pool = Some(pool);
        self
    }

    pub fn with_tiered_pool(mut self, pool: Rc<TieredMemoryPool>) -> Self {
        self.tiered_pool = Some(pool);
        self
    }

    pub fn with_registry(mut self, registry: Rc<QueryRegistry>) -> Self {
        self.registry = Some(registry);
        self
    }

    pub fn with_manager(mut self, manager: Rc<MemoryManager>) -> Self {
        self.manager = Some(manager);
        self
    }

    pub fn generate_report(&self) -> ResourceReport {
        let mut report = ResourceReport::new();

        if let Some(pool) = &self.pool {
            report = report.with_pool_stats(pool);
        }

        if let Some(pool) = &self.tiered_pool {
            report = report.with_tiered_pool_stats(pool);
        }

        if let Some(registry) = &self.registry {
            report = report.with_registry_stats(registry);
        }

        if let Some(manager) = &self.manager {
            report = report.with_manager_stats(manager);
        }

        report.calculate_health();

        {
            let mut history = self.history.borrow_mut();
            history.push(report.clone());

            if history.len() > self.max_history {
                history.remove(0);
            }
        }

        report
    }

    pub fn history(&self) -> Vec<ResourceReport> {
        self.history.borrow().clone()
    }

    pub fn latest_report(&self) -> Option<ResourceReport> {
        self.history.borrow().last().cloned()
    }

    pub fn clear_history(&self) {
        self.history.borrow_mut().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_report_creation() {
        let report = ResourceReport::new();
        assert_eq!(report.health, SystemHealth::Healthy);
        assert!(report.pool_stats.is_none());
    }

    #[test]
    fn test_health_calculation() {
        let pool = Rc::new(MemoryPool::new(1000));
        let mut report = ResourceReport::new().with_pool_stats(&pool);

        report.calculate_health();
        assert_eq!(report.health, SystemHealth::Healthy);
    }

    #[test]
    fn test_monitor_basic() {
        let pool = Rc::new(MemoryPool::new(1024 * 1024 * 1024));
        let monitor = ResourceMonitor::new(10).with_pool(pool);

        let report = monitor.generate_report();
        assert!(report.pool_stats.is_some());
        assert_eq!(report.health, SystemHealth::Healthy);
    }

    #[test]
    fn test_monitor_history() {
        let pool = Rc::new(MemoryPool::new(1024 * 1024 * 1024));
        let monitor = ResourceMonitor::new(5).with_pool(pool);

        for _ in 0..10 {
            monitor.generate_report();
        }

        let history = monitor.history();
        assert_eq!(history.len(), 5);
    }

    #[test]
    fn test_report_formatting() {
        let pool = Rc::new(MemoryPool::new(1024 * 1024 * 1024));
        let report = ResourceReport::new().with_pool_stats(&pool);

        let formatted = report.format();
        assert!(formatted.contains("RESOURCE USAGE REPORT"));
        assert!(formatted.contains("Memory Pool"));

        let compact = report.format_compact();
        assert!(compact.contains("Health"));
        assert!(compact.contains("Memory"));
    }
}
