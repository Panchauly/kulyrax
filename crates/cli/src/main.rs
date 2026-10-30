use clap::{Parser, Subcommand, ValueEnum};
use kulyrax_core::adapter::SkipReason;
use kulyrax_core::execution::stages::StageTimings;
use kulyrax_core::execution::{
    AdapterOutcome, AdapterReportError, ExecutionReport, ExecutionStatus,
};
use kulyrax_core::pipeline;
use kulyrax_core::replay::{ReplayReport, ReplayRunOptions};
use kulyrax_core::routing::StorageKind;
use kulyrax_core::runtime::{
    DryRunOutcome, DryRunReport, PROJECTION_DEPTH_EXCEEDS_RECOMMENDED_WARNING, ReloadOutcome,
};
use kulyrax_core::source::runner::{RunMode, SourceRunOptions, SourceRunReport, StoppedReason};

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

#[derive(Parser)]
#[command(name = "kulyrax")]
#[command(about = "Event-first projection engine", version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Clone, Copy, Default, PartialEq, Eq, ValueEnum)]
enum OutputFormat {
    #[default]
    Text,
    Json,
}

#[derive(Subcommand)]
enum Commands {
    /// Ask a running `run` or `ingest` process to re-read its config, routing and mappings
    Reload {
        /// The same config file the running process was started with.
        #[arg(long)]
        config: PathBuf,
        /// Seconds to wait for the process to answer.
        #[arg(long, default_value_t = 30)]
        timeout_secs: u64,
    },

    /// Create a runnable starter project (config, routing, a mapping, sample events)
    Init {
        /// Folder to create the project in; created if missing.
        #[arg(default_value = ".")]
        dir: PathBuf,
    },

    /// Run a single event (`--event`) or the continuous source loop (config `sources:`).
    Run {
        #[arg(long)]
        config: PathBuf,

        #[arg(long)]
        mappings: PathBuf,

        /// One-shot: project this single event file and exit (no source loop).
        #[arg(long)]
        event: Option<PathBuf>,

        /// Drain every configured source once, commit, and exit (subsumes `replay`).
        #[arg(long, default_value_t = false)]
        once: bool,

        /// Max events per batch, committed in one transaction per adapter
        /// (source loop only). Default 5,000.
        #[arg(long)]
        max_batch: Option<usize>,

        /// Continuous mode: how long a batch may wait to fill, in ms. A batch
        /// closes at `--max-batch` events or after this delay, whichever
        /// comes first. Default 100; 0 disables it.
        #[arg(long)]
        max_batch_delay_ms: Option<u64>,

        /// Poll interval in ms when all sources are caught up (continuous mode).
        #[arg(long)]
        poll_interval_ms: Option<u64>,

        /// Directory to park poison events; without it a persistently-failing batch halts the loop.
        #[arg(long)]
        dlq: Option<PathBuf>,

        #[arg(long, value_enum, default_value_t)]
        output: OutputFormat,
    },

    /// List configured event sources and their committed positions.
    Sources {
        #[arg(long)]
        config: PathBuf,

        #[arg(long, value_enum, default_value_t)]
        output: OutputFormat,
    },

    /// Run the gRPC ingestion service — producers stream events, get position acks.
    Ingest {
        #[arg(long)]
        config: PathBuf,

        #[arg(long)]
        mappings: PathBuf,

        /// Listen address: `tcp://0.0.0.0:50051` or a bare `host:port`.
        #[arg(long, default_value = "tcp://127.0.0.1:50051")]
        listen: String,

        /// Max events per batch, committed in one transaction per adapter.
        /// Default 5,000.
        #[arg(long)]
        max_batch: Option<usize>,

        /// How long a batch may wait to fill, in ms. A batch closes at
        /// `--max-batch` events or after this delay, whichever comes first.
        /// Default 100; 0 disables it.
        #[arg(long)]
        max_batch_delay_ms: Option<u64>,

        /// Directory to park poison events.
        #[arg(long)]
        dlq: Option<PathBuf>,

        #[arg(long, value_enum, default_value_t)]
        output: OutputFormat,
    },

    /// Show which adapters an event reaches and in what order (no writes)
    Explain {
        #[arg(long)]
        config: PathBuf,

        #[arg(long)]
        event: PathBuf,

        /// If set, run full Phase 8 validation (mappings + routing + capabilities).
        #[arg(long)]
        mappings: Option<PathBuf>,

        #[arg(long, value_enum, default_value_t)]
        output: OutputFormat,
    },

    /// Show what one event would write to each adapter, without writing anything
    DryRun {
        #[arg(long)]
        config: PathBuf,

        #[arg(long)]
        mappings: PathBuf,

        #[arg(long)]
        event: PathBuf,

        #[arg(long, value_enum, default_value_t)]
        output: OutputFormat,
    },

    /// Replay events from a directory (sorted *.json) or file (JSON / NDJSON)
    Replay {
        #[arg(long)]
        config: PathBuf,

        #[arg(long)]
        mappings: PathBuf,

        /// Event file or directory of event JSON files
        #[arg(long)]
        events: PathBuf,

        #[arg(long, value_enum, default_value_t)]
        output: OutputFormat,

        /// Max successful per-event rows in the report (failures always listed)
        #[arg(long)]
        max_per_event: Option<usize>,

        /// Print progress to stderr every N events
        #[arg(long)]
        progress_every: Option<usize>,

        /// Fail events with no routing rule before calling adapters
        #[arg(long, default_value_t = false)]
        validate_routing: bool,

        /// Events per batch, committed in one transaction per adapter.
        /// Default 5,000; 1 projects one event at a time.
        #[arg(long)]
        max_batch: Option<usize>,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let exit_code = match cli.command {
        Commands::Init { dir } => init_cmd(dir)?,
        Commands::Reload {
            config,
            timeout_secs,
        } => reload_cmd(config, timeout_secs)?,
        Commands::Run {
            config,
            mappings,
            event,
            once,
            max_batch,
            max_batch_delay_ms,
            poll_interval_ms,
            dlq,
            output,
        } => run_cmd(
            config,
            mappings,
            event,
            once,
            max_batch,
            max_batch_delay_ms,
            poll_interval_ms,
            dlq,
            output,
        )?,

        Commands::Sources { config, output } => sources_cmd(config, output)?,

        Commands::Ingest {
            config,
            mappings,
            listen,
            max_batch,
            max_batch_delay_ms,
            dlq,
            output,
        } => ingest_cmd(
            config,
            mappings,
            listen,
            max_batch,
            max_batch_delay_ms,
            dlq,
            output,
        )?,

        Commands::Explain {
            config,
            event,
            mappings,
            output,
        } => explain_cmd(config, event, mappings, output)?,

        Commands::DryRun {
            config,
            mappings,
            event,
            output,
        } => dry_run_cmd(config, mappings, event, output)?,

        Commands::Replay {
            config,
            mappings,
            events,
            output,
            max_per_event,
            progress_every,
            validate_routing,
            max_batch,
        } => replay_cmd(
            config,
            mappings,
            events,
            output,
            max_per_event,
            progress_every,
            validate_routing,
            max_batch,
        )?,
    };

    std::process::exit(exit_code);
}

// --------------------------------------------------
// Run — one-shot event, or the Phase 17 source loop
// --------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn run_cmd(
    config: PathBuf,
    mappings: PathBuf,
    event: Option<PathBuf>,
    once: bool,
    max_batch: Option<usize>,
    max_batch_delay_ms: Option<u64>,
    poll_interval_ms: Option<u64>,
    dlq: Option<PathBuf>,
    output: OutputFormat,
) -> Result<i32, Box<dyn std::error::Error>> {
    // Back-compat: `run --event <file>` stays a one-shot single-event run.
    if let Some(event) = event {
        let report = pipeline::run(
            &config,
            &mappings,
            &event,
            kulyrax_backends::register_default_backends,
        )?;
        render_report(&report, output)?;
        return Ok(exit_code_from_status(report.status));
    }

    let opts = SourceRunOptions {
        mode: if once {
            RunMode::Once
        } else {
            RunMode::Continuous {
                poll_interval: Duration::from_millis(poll_interval_ms.unwrap_or(1000)),
            }
        },
        max_batch: max_batch.unwrap_or(if once {
            kulyrax_core::replay::DEFAULT_REPLAY_BATCH
        } else {
            kulyrax_core::source::runner::DEFAULT_LIVE_BATCH
        }),
        max_batch_delay: batch_delay(max_batch_delay_ms),
        retry_budget: 3,
        dlq_dir: dlq,
    };

    let stop = Arc::new(AtomicBool::new(false));
    {
        let stop = Arc::clone(&stop);
        // Graceful shutdown: stop polling, finish the in-flight batch, commit, exit.
        let _ = ctrlc::set_handler(move || stop.store(true, Ordering::Relaxed));
    }

    let report = pipeline::run_source_loop(
        &config,
        &mappings,
        &opts,
        &stop,
        kulyrax_backends::register_default_backends,
        log_reload,
    )?;
    render_source_run_report(&report, output)?;
    Ok(match report.stopped_reason {
        StoppedReason::RetryExhausted => 1,
        _ if report.events_failed > report.events_dlq => 1,
        _ => 0,
    })
}

// --------------------------------------------------
// Ingest — the gRPC ingestion service (Phase 20)
// --------------------------------------------------

/// `--max-batch-delay-ms`, or the default.
fn batch_delay(ms: Option<u64>) -> Duration {
    ms.map(Duration::from_millis)
        .unwrap_or(kulyrax_core::source::runner::DEFAULT_BATCH_DELAY)
}

fn ingest_cmd(
    config: PathBuf,
    mappings: PathBuf,
    listen: String,
    max_batch: Option<usize>,
    max_batch_delay_ms: Option<u64>,
    dlq: Option<PathBuf>,
    output: OutputFormat,
) -> Result<i32, Box<dyn std::error::Error>> {
    let stop = Arc::new(AtomicBool::new(false));
    {
        let stop = Arc::clone(&stop);
        let _ = ctrlc::set_handler(move || stop.store(true, Ordering::Relaxed));
    }

    let opts = kulyrax_ingest::ServeOptions {
        listen,
        max_batch: max_batch.unwrap_or(kulyrax_core::source::runner::DEFAULT_LIVE_BATCH),
        max_batch_delay: batch_delay(max_batch_delay_ms),
        retry_budget: 3,
        dlq_dir: dlq,
        ..Default::default()
    };

    let report = kulyrax_ingest::serve(
        &config,
        &mappings,
        &opts,
        &stop,
        kulyrax_backends::register_default_backends,
    )?;
    render_source_run_report(&report, output)?;
    Ok(match report.stopped_reason {
        StoppedReason::RetryExhausted => 1,
        _ if report.events_failed > report.events_dlq => 1,
        _ => 0,
    })
}

// --------------------------------------------------
// Sources (Phase 17.4)
// --------------------------------------------------

fn sources_cmd(config: PathBuf, output: OutputFormat) -> Result<i32, Box<dyn std::error::Error>> {
    let sources = pipeline::list_sources(&config)?;
    match output {
        OutputFormat::Text => {
            println!("--- Sources ---");
            if sources.is_empty() {
                println!("(none configured)");
            }
            for (id, pos) in &sources {
                println!("{}  committed: {}", id, pos.as_deref().unwrap_or("(never)"));
            }
            println!("---------------");
        }
        OutputFormat::Json => {
            let json: Vec<_> = sources
                .iter()
                .map(|(id, pos)| serde_json::json!({ "id": id, "committed_position": pos }))
                .collect();
            println!("{}", serde_json::to_string_pretty(&json)?);
        }
    }
    Ok(0)
}

fn render_source_run_report(
    report: &SourceRunReport,
    output: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    match output {
        OutputFormat::Text => {
            println!("--- Source Run ---");
            for b in &report.batches {
                println!(
                    "[{}] {}..{} — {} events (created {}, updated {}, deleted {}, skipped {}, failed {}, dlq {}, retries {}) — {}",
                    b.source_id,
                    b.position_from.as_deref().unwrap_or("(start)"),
                    b.position_to,
                    b.events,
                    b.created,
                    b.updated,
                    b.deleted,
                    b.skipped,
                    b.failed,
                    b.dlq,
                    b.retries,
                    if b.committed {
                        "committed"
                    } else {
                        "NOT committed"
                    }
                );
            }
            println!(
                "Processed: {}  Succeeded: {}  Failed: {}  DLQ: {}  Batches committed: {}",
                report.events_processed,
                report.events_succeeded,
                report.events_failed,
                report.events_dlq,
                report.batches_committed
            );
            println!("Stopped: {:?}", report.stopped_reason);
            if let Some(ref p) = report.halt_position {
                println!("Halt position: {}", p);
            }
            render_stage_timings(&report.timings);
            println!("------------------");
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(report)?);
        }
    }
    Ok(())
}

fn render_stage_timings(t: &StageTimings) {
    println!(
        "Time per stage (dispatch contains adapter and batch, which contain read/write/commit/log/apply; schema runs once, before ingesting; sync runs in the background):"
    );
    for (name, s) in [
        ("source read", t.source_read),
        ("dispatch", t.dispatch),
        ("adapter", t.adapter),
        ("batch", t.batch),
        ("schema", t.schema),
        ("read", t.read),
        ("write", t.write),
        ("commit", t.commit),
        ("checkpoint", t.checkpoint),
        ("log", t.log),
        ("apply", t.apply),
        ("sync", t.sync),
    ] {
        if s.calls == 0 {
            continue;
        }
        println!(
            "  {name:<12} {:>10} calls  {:>12.1} ms  {:>10.1} µs/call",
            s.calls,
            s.total_us as f64 / 1000.0,
            s.total_us as f64 / s.calls as f64
        );
    }
    if t.adapter.calls > 0 {
        let inner = t.read.total_us
            + t.write.total_us
            + t.commit.total_us
            + t.log.total_us
            + t.apply.total_us;
        let other = (t.adapter.total_us + t.batch.total_us).saturating_sub(inner);
        println!(
            "  adapter and batch time outside read/write/commit/log/apply (mapping, connection, begin): {:.1} ms, {:.1} µs/event",
            other as f64 / 1000.0,
            other as f64 / t.adapter.calls as f64
        );
        let outside = t
            .dispatch
            .total_us
            .saturating_sub(t.adapter.total_us + t.batch.total_us);
        println!(
            "  dispatch time outside adapter and batch calls (routing, reports): {:.1} ms, {:.1} µs/event",
            outside as f64 / 1000.0,
            outside as f64 / t.adapter.calls as f64
        );
    }
}

// --------------------------------------------------
// Replay (Phase 7)
// --------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn replay_cmd(
    config: PathBuf,
    mappings: PathBuf,
    events: PathBuf,
    output: OutputFormat,
    max_per_event: Option<usize>,
    progress_every: Option<usize>,
    validate_routing: bool,
    max_batch: Option<usize>,
) -> Result<i32, Box<dyn std::error::Error>> {
    let opts = ReplayRunOptions {
        max_per_event_summaries: max_per_event,
        progress_interval: progress_every,
        validate_routing,
        max_batch,
    };
    let report = pipeline::replay(
        &config,
        &mappings,
        &events,
        &opts,
        kulyrax_backends::register_default_backends,
    )?;

    render_replay_report(&report, output)?;

    let exit = if report.events_failed > 0 { 1 } else { 0 };
    Ok(exit)
}

fn render_replay_report(
    report: &ReplayReport,
    output: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    match output {
        OutputFormat::Text => {
            println!("--- Replay Report ---");
            println!(
                "Processed: {}  Succeeded: {}  Failed: {}",
                report.events_processed, report.events_succeeded, report.events_failed
            );
            println!(
                "Stopped early: {}  Duration: {} ms",
                report.stopped_early,
                report.duration_ms.unwrap_or(0)
            );
            if let Some(ref id) = report.first_failure_event_id {
                println!("First failure event_id: {}", id);
            }
            if report.per_event_summaries_omitted > 0 {
                println!(
                    "Per-event rows omitted (success cap): {}",
                    report.per_event_summaries_omitted
                );
            }
            println!();
            for e in &report.per_event {
                println!(
                    "{} ({}) — {}",
                    e.event_id,
                    e.event_type,
                    format_status(e.status)
                );
            }
            println!("---------------------");
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(report)?);
        }
    }
    Ok(())
}

// --------------------------------------------------
// Explain (routing only)
// --------------------------------------------------

fn explain_cmd(
    config: PathBuf,
    event: PathBuf,
    mappings: Option<PathBuf>,
    output: OutputFormat,
) -> Result<i32, Box<dyn std::error::Error>> {
    let result = pipeline::explain(&config, &event, mappings.as_deref())?;

    if result.depth_warning {
        eprintln!("{}", PROJECTION_DEPTH_EXCEEDS_RECOMMENDED_WARNING);
    }

    match output {
        OutputFormat::Text => {
            println!("--- Explain ---");
            println!(
                "Event: {} ({})",
                result.event.event_type, result.event.event_id
            );
            println!(
                "Routed adapters: {}",
                if result.routed_adapters.is_empty() {
                    "(none)".to_string()
                } else {
                    result.routed_adapters.join(", ")
                }
            );
            println!(
                "Execution order (dependencies resolved): {}",
                if result.execution_order.is_empty() {
                    "(none)".to_string()
                } else {
                    result.execution_order.join(", ")
                }
            );
            if !result.execution_layers.is_empty() {
                println!("Execution layers:");
                for (layer, adapters) in &result.execution_layers {
                    println!("  Layer {}: {}", layer, adapters.join(", "));
                }
            }
            println!("----------------");
        }
        OutputFormat::Json => {
            let execution_layers: Vec<serde_json::Value> = result
                .execution_layers
                .iter()
                .map(|(layer, adapters)| {
                    serde_json::json!({
                        "layer": layer,
                        "adapters": adapters
                    })
                })
                .collect();
            let warnings: Vec<&str> = if result.depth_warning {
                vec![PROJECTION_DEPTH_EXCEEDS_RECOMMENDED_WARNING]
            } else {
                Vec::new()
            };
            let json = serde_json::json!({
                "event_type": result.event.event_type,
                "event_id": result.event.event_id,
                "routed_adapters": result.routed_adapters,
                "execution_order": result.execution_order,
                "execution_layers": execution_layers,
                "warnings": warnings
            });
            println!("{}", serde_json::to_string_pretty(&json)?);
        }
    }

    Ok(0)
}

// --------------------------------------------------
// Reload (ask a running process to re-read its project)
// --------------------------------------------------

fn reload_cmd(config: PathBuf, timeout_secs: u64) -> Result<i32, Box<dyn std::error::Error>> {
    let outcome = pipeline::request_reload(&config, Duration::from_secs(timeout_secs))?;
    Ok(match outcome {
        ReloadOutcome::Applied { message } => {
            println!("reload applied: {message}");
            0
        }
        ReloadOutcome::Rejected { reason } => {
            eprintln!(
                "reload rejected, still running the previous configuration:
  {reason}"
            );
            1
        }
        ReloadOutcome::NotPickedUp => {
            eprintln!(
                "no running process answered within {timeout_secs}s. Is `kulyrax run` (without --once) or `kulyrax ingest` running with {}?",
                config.display()
            );
            2
        }
    })
}

/// How a running process reports each reload on stderr.
fn log_reload(outcome: &ReloadOutcome) {
    match outcome {
        ReloadOutcome::Applied { message } => eprintln!("kulyrax: reload applied ({message})"),
        ReloadOutcome::Rejected { reason } => {
            eprintln!("kulyrax: reload rejected, keeping the previous configuration: {reason}")
        }
        ReloadOutcome::NotPickedUp => {}
    }
}

// --------------------------------------------------
// Init (starter project)
// --------------------------------------------------

fn init_cmd(dir: PathBuf) -> Result<i32, Box<dyn std::error::Error>> {
    let written = match pipeline::init_project(&dir) {
        Ok(written) => written,
        Err(e) => {
            eprintln!("error: {e}");
            return Ok(1);
        }
    };
    println!("Created a Kulyrax project in {}:", dir.display());
    for path in &written {
        println!("  {}", path.display());
    }
    println!();
    println!("Next:");
    if dir != std::path::Path::new(".") {
        println!("  cd {}", dir.display());
    }
    println!("  kulyrax run --config config.yaml --mappings mappings --once");
    println!("  # then look in ./out/users/");
    Ok(0)
}

// --------------------------------------------------
// Dry-run (resolve writes, never perform them)
// --------------------------------------------------

fn dry_run_cmd(
    config: PathBuf,
    mappings: PathBuf,
    event: PathBuf,
    output: OutputFormat,
) -> Result<i32, Box<dyn std::error::Error>> {
    let report = pipeline::dry_run(&config, &mappings, &event)?;
    match output {
        OutputFormat::Text => print_dry_run_report(&report),
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
    }
    let any_failed = report
        .adapters
        .iter()
        .any(|a| matches!(a.outcome, DryRunOutcome::Failed { .. }));
    Ok(if any_failed { 1 } else { 0 })
}

fn print_dry_run_report(report: &DryRunReport) {
    println!("--- Dry Run (nothing was written) ---");
    println!("Event: {} ({})", report.event_id, report.event_type);
    if report.adapters.is_empty() {
        println!("No adapter is routed for this event type.");
    }
    for (i, adapter) in report.adapters.iter().enumerate() {
        let kind = adapter
            .kind
            .map_or_else(|| "unknown kind".to_string(), |k| k.to_string());
        println!();
        println!("{}. {} ({kind})", i + 1, adapter.adapter_id);
        match &adapter.outcome {
            DryRunOutcome::Planned(w) => {
                let facet = if w.facet.is_empty() {
                    String::new()
                } else {
                    format!(", facet {}", w.facet)
                };
                println!(
                    "   would {:?} {} key {}{facet} (on_existing: {:?}, v{} -> v{})",
                    w.operation,
                    w.target,
                    w.entity_key,
                    w.on_existing,
                    w.source_version,
                    w.projected_version
                );
                println!("   values: {}", w.values);
            }
            DryRunOutcome::Failed { error } => println!("   would fail: {error}"),
            DryRunOutcome::NotPreviewable { reason } => println!("   not previewable: {reason}"),
        }
    }
    println!();
    println!(
        "Guard state was not read, so a live run may still skip these writes as \
         already projected or stale."
    );
}

// --------------------------------------------------
// Rendering
// --------------------------------------------------

fn render_report(
    report: &ExecutionReport,
    output: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    match output {
        OutputFormat::Text => print_text_report(report),
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(report)?);
        }
    }
    Ok(())
}

fn print_text_report(report: &ExecutionReport) {
    println!("--- Execution Report ---");
    println!("Event: {} ({})", report.event_type, report.event_id);
    println!("Trace: {}", report.trace_id);
    println!("Status: {}", format_status(report.status));
    println!("Duration: {} ms", report.duration_ms.unwrap_or(0));
    println!();

    for r in &report.adapter_reports {
        match r.outcome {
            AdapterOutcome::Created => {
                println!(
                    "✓ {} ({}) — created — {} ms",
                    r.adapter_id,
                    format_storage_kind(r.storage_kind),
                    r.duration_ms.unwrap_or(0)
                );
            }
            AdapterOutcome::Updated => {
                println!(
                    "✓ {} ({}) — updated — {} ms",
                    r.adapter_id,
                    format_storage_kind(r.storage_kind),
                    r.duration_ms.unwrap_or(0)
                );
            }
            AdapterOutcome::Deleted => {
                println!(
                    "✓ {} ({}) — deleted — {} ms",
                    r.adapter_id,
                    format_storage_kind(r.storage_kind),
                    r.duration_ms.unwrap_or(0)
                );
            }
            AdapterOutcome::Skipped { reason } => {
                println!(
                    "~ {} ({}) — skipped: {}",
                    r.adapter_id,
                    format_storage_kind(r.storage_kind),
                    format_skip_reason(reason)
                );
            }
            AdapterOutcome::Failed => {
                println!(
                    "✗ {} ({}) — {}",
                    r.adapter_id,
                    format_storage_kind(r.storage_kind),
                    extract_error(r.error.as_ref())
                );
            }
        }
    }

    println!("------------------------");
}

fn format_status(status: ExecutionStatus) -> &'static str {
    match status {
        ExecutionStatus::Succeeded => "succeeded",
        ExecutionStatus::Failed => "failed",
    }
}

fn format_storage_kind(kind: StorageKind) -> &'static str {
    match kind {
        StorageKind::Sql => "sql",
        StorageKind::Document => "document",
        StorageKind::KeyValue => "keyvalue",
        StorageKind::Graph => "graph",
        StorageKind::Custom => "custom",
    }
}

fn extract_error(err: Option<&AdapterReportError>) -> &str {
    match err {
        Some(AdapterReportError::WriteFailed { message }) => message,
        Some(AdapterReportError::UnsupportedVersion { message }) => message,
        None => "unknown",
    }
}

fn format_skip_reason(reason: SkipReason) -> &'static str {
    match reason {
        SkipReason::AlreadyProjected => "already projected",
        SkipReason::StaleSequence => "stale sequence",
        SkipReason::UnsupportedVersion => "unsupported version: no upcaster chain",
        SkipReason::AlreadyDeleted => "already deleted",
        SkipReason::Tombstoned => "permanently tombstoned",
        SkipReason::EntityAbsent => "entity absent (facet update with no created entity)",
    }
}

fn exit_code_from_status(status: ExecutionStatus) -> i32 {
    match status {
        ExecutionStatus::Succeeded => 0,
        ExecutionStatus::Failed => 1,
    }
}
