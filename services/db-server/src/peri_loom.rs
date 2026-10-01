#[tokio::main]
async fn main() -> std::process::ExitCode {
    db_server::cli::run_cli().await
}
