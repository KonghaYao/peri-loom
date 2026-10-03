// Same-machine embedded baseline for the Simple point-read workload.
// Compile against the workspace's release turso_core rlib, then pass a fresh
// SQLite database path as the first argument. Use a new path for each round.
use std::{sync::Arc, time::Instant};
use turso_core::{Database, OpenOptions, SqliteDialect, StepResult, SyncMode};
fn run(db: &Database, conn: &std::sync::Arc<turso_core::Connection>, sql: &str) -> i64 {
    let mut stmt = conn.prepare(sql).unwrap();
    let mut value = 0;
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                value = stmt.row().unwrap().get_value(0).as_int().unwrap();
            }
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                db.io.step().unwrap();
            }
            StepResult::Done => break,
            StepResult::Busy | StepResult::Interrupt => panic!("unexpected step"),
        }
    }
    value
}
fn pct(v: &mut Vec<f64>, p: f64) -> f64 {
    v.sort_by(|a, b| a.total_cmp(b));
    v[((v.len() - 1) as f64 * p).round() as usize]
}
fn main() {
    let path = std::env::args().nth(1).expect("db path");
    let io = Arc::new(turso_core::UnixIO::new().unwrap());
    let db = Database::open(io, &path, OpenOptions::new(Arc::new(SqliteDialect))).unwrap();
    let conn = db.connect().unwrap();
    conn.set_sync_mode(SyncMode::Full);
    conn.set_sync_type(turso_core::io::FileSyncType::FullFsync);
    conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO bench VALUES (1, 42)").unwrap();
    let sql = "SELECT v FROM bench WHERE id=1";
    for _ in 0..100 {
        assert_eq!(run(&db, &conn, sql), 42)
    }
    for variant in ["reuse_connection", "new_connection"] {
        let mut us = Vec::new();
        let wall = Instant::now();
        for _ in 0..1000 {
            let start = Instant::now();
            if variant == "new_connection" {
                let c = db.connect().unwrap();
                c.set_sync_mode(SyncMode::Full);
                c.set_sync_type(turso_core::io::FileSyncType::FullFsync);
                assert_eq!(run(&db, &c, sql), 42)
            } else {
                assert_eq!(run(&db, &conn, sql), 42)
            }
            us.push(start.elapsed().as_secs_f64() * 1e6);
        }
        let elapsed = wall.elapsed().as_secs_f64();
        let mut c = us.clone();
        let p50 = pct(&mut c, 0.5);
        let p95 = pct(&mut c, 0.95);
        let p99 = pct(&mut c, 0.99);
        println!(
            "{} n={} p50_us={:.3} p95_us={:.3} p99_us={:.3} max_us={:.3} wall_rps={:.1}",
            variant,
            us.len(),
            p50,
            p95,
            p99,
            us.iter().copied().fold(0., f64::max),
            us.len() as f64 / elapsed
        );
    }
}
