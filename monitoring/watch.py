"""
Live terminal dashboard for monitoring PySpark Structured Streaming pipeline.
Tails monitoring/metrics.log and renders a real-time updating Rich table every 2 seconds
showing throughput, batch duration, Kafka consumer lag, watermark, and health status.
"""

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import sys
import time
from typing import Any, Dict

from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

# Path bootstrap to allow running as `python monitoring/watch.py` or `python -m monitoring.watch`
repo_root = Path(__file__).resolve().parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from config import METRICS_LOG_PATH


def read_latest_metrics(log_file: Path, file_pos: int, latest_stats: Dict[str, Dict[str, Any]]) -> int:
    """
    Read new JSON lines appended to log_file starting at file_pos.
    Updates latest_stats dictionary in-place keyed by query_name.
    Returns the new file position.
    """
    if not log_file.exists():
        return file_pos

    try:
        current_size = log_file.stat().st_size
        # Handle file rotation or truncation
        if current_size < file_pos:
            file_pos = 0

        with open(log_file, "r", encoding="utf-8", errors="replace") as f:
            f.seek(file_pos)
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    qname = record.get("query_name")
                    if qname:
                        latest_stats[qname] = record
                except Exception:
                    pass
            new_pos = f.tell()
            return new_pos
    except Exception:
        return file_pos


def build_dashboard_renderable(
    latest_stats: Dict[str, Dict[str, Any]],
    log_path: Path,
    last_update_time: str,
) -> Panel:
    """Build the Rich renderable containing pipeline header and query performance table."""
    table = Table(
        title="Real-Time Streaming Queries Health & Throughput",
        header_style="bold cyan",
        border_style="blue",
        expand=True,
    )

    table.add_column("Query Name", style="bold white", width=16)
    table.add_column("Batch", justify="right", width=8)
    table.add_column("Input (rows/s)", justify="right", width=15)
    table.add_column("Processed (rows/s)", justify="right", width=18)
    table.add_column("Duration (ms)", justify="right", width=14)
    table.add_column("Consumer Lag", justify="right", width=14)
    table.add_column("State Rows", justify="right", width=12)
    table.add_column("Watermark", justify="center", width=22)
    table.add_column("Status", justify="center", width=18)

    if not latest_stats:
        table.add_row(
            "[dim]Waiting...[/dim]",
            "-",
            "-",
            "-",
            "-",
            "-",
            "-",
            "-",
            "[yellow]NO DATA YET[/yellow]",
        )
    else:
        # Sort queries: raw_trades first, then aggregations
        ordered_queries = sorted(
            latest_stats.keys(),
            key=lambda k: (0 if "raw" in k else 1, k),
        )

        for qname in ordered_queries:
            st = latest_stats[qname]
            batch_id = str(st.get("batch_id", 0))
            in_rate = f"{st.get('input_rows_per_second', 0.0):,.1f}"
            proc_rate = f"{st.get('processed_rows_per_second', 0.0):,.1f}"
            duration = float(st.get("batch_duration_ms", 0.0))
            lag = int(st.get("kafka_consumer_lag", 0))
            state_rows = f"{st.get('state_rows', 0):,}"
            watermark_raw = st.get("watermark")
            watermark = str(watermark_raw).replace("T", " ")[:19] if watermark_raw else "N/A"
            falling_behind = st.get("falling_behind", False)

            # Style duration and lag
            duration_str = f"[bold red]{duration:,.0f}[/bold red]" if falling_behind else f"{duration:,.0f}"
            lag_str = f"[bold yellow]{lag:,}[/bold yellow]" if lag > 0 else f"[green]{lag}[/green]"

            # Status pill
            if falling_behind:
                status = "[bold white on red] FALLING BEHIND [/bold white on red]"
            else:
                status = "[bold green]HEALTHY[/bold green]"

            table.add_row(
                qname,
                batch_id,
                in_rate,
                proc_rate,
                duration_str,
                lag_str,
                state_rows,
                watermark,
                status,
            )

    header_text = Text()
    header_text.append("Observability Source: ", style="bold")
    header_text.append(f"{log_path}\n", style="cyan")
    header_text.append("Last Refreshed     : ", style="bold")
    header_text.append(f"{last_update_time} | Refresh: 2s (Press Ctrl+C to exit)\n", style="magenta")

    panel = Panel(
        table,
        title="[bold yellow]== Stock Market Real-Time Streaming Pipeline Monitor (Phase 4) ==[/bold yellow]",
        subtitle=header_text,
        border_style="bright_blue",
    )
    return panel


def main() -> None:
    # Ensure UTF-8 output streams on Windows terminals
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

    parser = argparse.ArgumentParser(
        description="Tail monitoring/metrics.log and render a live terminal health dashboard.",
    )
    parser.add_argument(
        "--log-path",
        type=str,
        default=METRICS_LOG_PATH,
        help=f"Path to metrics.log (default: {METRICS_LOG_PATH})",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=2.0,
        help="Dashboard refresh interval in seconds (default: 2.0)",
    )
    args = parser.parse_args()

    log_path = Path(args.log_path).resolve()
    console = Console()

    latest_stats: Dict[str, Dict[str, Any]] = {}
    file_pos = 0

    console.print(f"[bold green]Starting metrics live watcher on:[/] {log_path}")
    console.print("[dim]Waiting for streaming progress events... (Press Ctrl+C to quit)[/dim]\n")

    try:
        with Live(console=console, refresh_per_second=2, screen=False) as live:
            while True:
                file_pos = read_latest_metrics(log_path, file_pos, latest_stats)
                now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                panel = build_dashboard_renderable(latest_stats, log_path, now_str)
                live.update(panel)
                time.sleep(args.interval)
    except KeyboardInterrupt:
        console.print("\n[bold yellow]Dashboard watcher stopped by user.[/bold yellow]")


if __name__ == "__main__":
    main()
