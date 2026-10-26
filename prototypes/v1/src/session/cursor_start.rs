use super::*;
use crate::session::CursorFirstPage;

impl SessionLane {
    /// Hold the connection across preparation and the first page. The first
    /// page uses the same completion/cancellation code as every later fetch;
    /// there is no independently reusable half-open cursor between the calls.
    #[allow(clippy::too_many_arguments)]
    pub fn open_cursor_first(
        &self,
        database: &RemoteDatabase,
        sql: &str,
        args: &[Value],
        named_args: &[(String, Value)],
        page_size: usize,
        writing: bool,
        budget: &Budget,
    ) -> Result<CursorFirstPage> {
        let _reads = crate::authority::request_reads::Scope::enter();
        let deadline = RequestDeadline::new(budget)?;
        anyhow::ensure!(
            (1..=256).contains(&page_size),
            "cursor page size exceeds limit"
        );
        let connection = self
            .lock(database, &deadline, false)
            .map_err(|error| deadline.evidence.failure(error))?;
        let native = connection.as_ref().ok_or(AdmissionError::Quarantine)?;
        let result = if writing {
            execution::open_write_cursor(
                database,
                native,
                &self.control,
                sql,
                args,
                named_args,
                budget,
            )
            .map_err(|error| deadline.evidence.failure(error))
            .and_then(|(cursor, cols)| {
                self.step_write_cursor(database, native, cursor, page_size)
                    .map(|page| CursorFirstPage { cols, page })
            })
        } else {
            execution::open_read_cursor(
                database,
                native,
                &self.control,
                sql,
                args,
                named_args,
                budget,
            )
            .map_err(|error| deadline.evidence.failure(error))
            .and_then(|(cursor, cols)| {
                self.step_read_cursor(database, native, cursor, page_size)
                    .map(|(rows, done)| CursorFirstPage {
                        cols,
                        page: WriteCursorPage {
                            rows,
                            done,
                            affected_row_count: None,
                            last_insert_rowid: None,
                        },
                    })
            })
        };
        drop(connection);
        self.finish_call(database, result)
    }
}
