//! 测试共用的候选 / 预算工厂（仅测试编译）。
//!
//! 放在独立模块而不是各测试模块内部，是为了让多个测试模块共享同一套构造逻辑
//! （`mod tests` 是私有的，跨模块无法复用其中定义的辅助函数）。

use domain::{ResourceBudget, WorkerCapacity, WorkerId, WorkerResourceUsage, WorkerState};

use crate::WorkerCandidate;

/// 测试基准容量：8000 milli-core / 16 GiB / 4096 FD / 100 GiB / 128 进程位 / 20000 IOPS。
pub fn capacity() -> WorkerCapacity {
    WorkerCapacity::new(8000, 16_384, 4096, 102_400, 128, 20_000)
}

/// 构造一个 `ACTIVE` 候选，容量取 [`capacity`]，占用只给 CPU / 内存。
pub fn candidate(id: &str, cpu_used: u64, memory_used: u64) -> WorkerCandidate {
    let capacity = capacity();
    WorkerCandidate {
        worker_id: WorkerId::new(id),
        state: WorkerState::Active,
        region: "r1".to_string(),
        zone: "z1".to_string(),
        capacity,
        usage: WorkerResourceUsage::new(
            capacity,
            ResourceBudget::new(cpu_used, memory_used, 100, 1000, 4, 1000),
        ),
        db_process_count: 4,
        respawning: false,
    }
}

/// 只走内存维的候选：`memory_used / 16000` 即为其最大利用率。
///
/// 内存总量被刻意压到 16000，便于用整数构造精确的水位边界
/// （CPU 维同步给出不会成为瓶颈的值）。
#[must_use]
pub fn memory_candidate(id: &str, memory_used: u64) -> WorkerCandidate {
    let capacity = WorkerCapacity::new(64_000, 16_000, 4096, 102_400, 128, 20_000);
    WorkerCandidate {
        worker_id: WorkerId::new(id),
        state: WorkerState::Active,
        region: "r1".to_string(),
        zone: "z1".to_string(),
        capacity,
        usage: WorkerResourceUsage::new(
            capacity,
            ResourceBudget::new(memory_used, memory_used, 100, 1000, 4, 1000),
        ),
        db_process_count: 4,
        respawning: false,
    }
}

/// 沿内存维推进一个 DB 的预算（默认 `process_slots = 1`，一个 DB = 一个进程）。
pub fn db_budget(memory_mib: u64) -> ResourceBudget {
    ResourceBudget::new(100, memory_mib, 16, 64, 1, 20)
}
