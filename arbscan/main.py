from __future__ import annotations

import argparse
import asyncio
import logging
import signal
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Set, Tuple

from arbscan.arb import compute_arb_signals
from arbscan.config import AppConfig, load_config
from arbscan.connectors import KalshiConnector, LimitlessStubConnector, PolymarketConnector
from arbscan.connectors.base import QuoteUpdate
from arbscan.dedupe import DedupeCache
from arbscan.validation import QuoteValidator
from arbscan.mapping import MappingFile, load_mappings
from arbscan.models import MarketQuote, MappedEvent
from arbscan.notify import notify

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EventIndex:
    by_venue_market: Dict[Tuple[str, str], Set[str]]
    events: Dict[str, MappedEvent]


def build_event_index(mapping_file: MappingFile) -> EventIndex:
    by_venue_market: Dict[Tuple[str, str], Set[str]] = {}
    events: Dict[str, MappedEvent] = {}

    for event in mapping_file.events:
        events[event.canonical_event_id] = event
        for ref in event.refs:
            keys = [ref.market_id]
            if ref.yes_id:
                keys.append(ref.yes_id)
            if ref.no_id:
                keys.append(ref.no_id)
            for key in keys:
                by_venue_market.setdefault((ref.venue, key), set()).add(event.canonical_event_id)

    return EventIndex(by_venue_market=by_venue_market, events=events)


def _fees(config: AppConfig) -> Dict[str, float]:
    return {
        "polymarket": config.fee_polymarket,
        "kalshi": config.fee_kalshi,
        "limitless": config.fee_limitless,
    }


def _slippages(config: AppConfig) -> Dict[str, float]:
    return {
        "polymarket": config.slippage_polymarket,
        "kalshi": config.slippage_kalshi,
        "limitless": config.slippage_limitless,
    }


async def run_scanner(config: AppConfig, mappings_path: Path) -> None:
    mapping_file = load_mappings(str(mappings_path))
    index = build_event_index(mapping_file)

    queue: asyncio.Queue[QuoteUpdate] = asyncio.Queue()
    quotes: Dict[Tuple[str, str, str], MarketQuote] = {}
    dedupe = DedupeCache(
        config.dedupe_seconds, config.margin_step_resend, config.size_step_resend
    )
    validator = QuoteValidator()

    async def on_update(update: QuoteUpdate) -> None:
        if validator.validate(update):
            await queue.put(update)

    polymarket_refs = [ref for event in mapping_file.events for ref in event.refs if ref.venue == "polymarket"]
    kalshi_refs = [ref for event in mapping_file.events for ref in event.refs if ref.venue == "kalshi"]
    limitless_refs = [ref for event in mapping_file.events for ref in event.refs if ref.venue == "limitless"]

    connectors = [
        PolymarketConnector(config, on_update, polymarket_refs),
        KalshiConnector(config, on_update, kalshi_refs),
        LimitlessStubConnector(config, on_update, limitless_refs),
    ]

    async def connector_runner(connector) -> None:
        if config.mock_mode:
            await connector.run()
            return

        backoff = 1
        while True:
            try:
                await connector.run()
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("Connector %s failed: %s", connector.venue, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def process_updates() -> None:
        while True:
            update = await queue.get()
            key = (update.venue, update.market_id, update.outcome)
            quotes[key] = update.quote

            event_ids = index.by_venue_market.get((update.venue, update.market_id), set())
            for event_id in event_ids:
                event = index.events[event_id]
                signals = compute_arb_signals(
                    event=event,
                    quotes=quotes,
                    fees=_fees(config),
                    slippages=_slippages(config),
                    threshold=config.arb_threshold,
                    now=datetime.utcnow(),
                )
                for signal in signals:
                    if dedupe.should_send(signal, signal.timestamp):
                        notify(signal, config.slack_webhook_url)

    connector_tasks = [asyncio.create_task(connector_runner(connector)) for connector in connectors]
    processor_task = asyncio.create_task(process_updates())
    tasks = connector_tasks + [processor_task]

    stop_event = asyncio.Event()

    def _handle_signal(*_):
        logger.info("Shutdown signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            signal.signal(sig, lambda *_: _handle_signal())

    if config.mock_mode:
        await asyncio.gather(*connector_tasks)
        await asyncio.sleep(0.1)
        stop_event.set()

    await stop_event.wait()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def validate_mappings(config: AppConfig, mappings_path: Path) -> int:
    from arbscan.validate import validate_mappings as run_validation

    errors = run_validation(config, str(mappings_path))
    if errors:
        for error in errors:
            logger.error("Mapping error: %s", error.message)
        return 1
    logger.info("Mappings are valid: %s", mappings_path)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="arbscan")
    sub = parser.add_subparsers(dest="command", required=True)

    run_cmd = sub.add_parser("run", help="Run the arb scanner")
    run_cmd.add_argument("--mock", action="store_true", help="Use mock data")
    run_cmd.add_argument(
        "--mappings",
        default="mappings.yml",
        help="Path to mappings.yml",
    )

    validate_cmd = sub.add_parser("validate-mappings", help="Validate mappings.yml")
    validate_cmd.add_argument(
        "--mappings",
        default="mappings.yml",
        help="Path to mappings.yml",
    )

    discover_cmd = sub.add_parser("discover", help="Suggest potential market matches")
    discover_cmd.add_argument(
        "--output",
        default="suggestions.yml",
        help="Path to write suggestions.yml",
    )
    discover_cmd.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Max number of suggestions",
    )

    kalshi_search_cmd = sub.add_parser("kalshi-search", help="Search Kalshi markets")
    kalshi_search_cmd.add_argument("--query", required=True, help="Search query")
    kalshi_search_cmd.add_argument("--limit", type=int, default=50, help="Max results")
    kalshi_search_cmd.add_argument("--series-ticker", help="Filter by series ticker")
    kalshi_search_cmd.add_argument("--status", default="open", help="Market status filter")
    kalshi_search_cmd.add_argument("--close-after", help="Filter close_time >= ISO8601")
    kalshi_search_cmd.add_argument("--close-before", help="Filter close_time <= ISO8601")
    kalshi_search_cmd.add_argument("--out", help="Write JSON output to file")

    kalshi_series_cmd = sub.add_parser("kalshi-series", help="List markets in a Kalshi series")
    kalshi_series_cmd.add_argument("--ticker", required=True, help="Series ticker")

    polymarket_search_cmd = sub.add_parser(
        "polymarket-search", help="Search Polymarket markets"
    )
    polymarket_search_cmd.add_argument("--query", required=True, help="Search query")
    polymarket_search_cmd.add_argument("--limit", type=int, default=50, help="Max results")
    polymarket_search_cmd.add_argument("--out", help="Write JSON output to file")

    generate_cmd = sub.add_parser(
        "generate-mappings", help="Generate mappings skeleton from discovery output"
    )
    generate_cmd.add_argument("--kalshi", help="Kalshi markets JSON from kalshi-search")
    generate_cmd.add_argument(
        "--polymarket", help="Polymarket markets JSON from polymarket-search"
    )
    generate_cmd.add_argument("--out", default="mappings.skeleton.yml", help="Output path")

    scan_cmd = sub.add_parser("scan-catalog", help="Scan markets and build matches database")
    scan_cmd.add_argument("--db", help="SQLite DB path override")
    scan_cmd.add_argument("--sports-only", action="store_true", help="Scan sports-only markets")

    live_cmd = sub.add_parser("run-live", help="Run live arb detection for matched pairs")
    live_cmd.add_argument("--db", help="SQLite DB path override")
    live_cmd.add_argument("--sports-only", action="store_true", help="Run live sports-only matches")

    review_cmd = sub.add_parser("review", help="Export REVIEW matches to CSV")
    review_cmd.add_argument("--min-score", type=float, default=0.85, help="Minimum score")
    review_cmd.add_argument("--out", default="review_matches.csv", help="CSV output path")
    review_cmd.add_argument("--db", help="SQLite DB path override")

    explain_event_cmd = sub.add_parser(
        "explain-event-match", help="Explain a Kalshi/Polymarket event match"
    )
    explain_event_cmd.add_argument("kalshi_event", help="Kalshi event ticker")
    explain_event_cmd.add_argument("polymarket_event", help="Polymarket event id or slug")

    explain_outcome_cmd = sub.add_parser(
        "explain-outcome-match", help="Explain a Kalshi/Polymarket outcome match"
    )
    explain_outcome_cmd.add_argument("kalshi_market", help="Kalshi market ticker")
    explain_outcome_cmd.add_argument("polymarket_market", help="Polymarket market id")

    debug_fed_cmd = sub.add_parser(
        "debug-fed-decision", help="Debug KX Fed decision event overlap"
    )

    debug_sports_cmd = sub.add_parser(
        "debug-sports-match", help="Debug a Kalshi/Polymarket sports matchup"
    )
    debug_sports_cmd.add_argument("kalshi_market", help="Kalshi market ticker or URL")
    debug_sports_cmd.add_argument("polymarket_market", help="Polymarket market id/slug or URL")

    validate_fed_cmd = sub.add_parser(
        "validate-fed-example", help="Validate Fed decision canonical predicates"
    )

    stats_cmd = sub.add_parser("matcher-stats", help="Report matcher stats at thresholds")
    stats_cmd.add_argument("--db", help="SQLite DB path override")

    debug_cmd = sub.add_parser("debug-db", help="Print DB counts for markets/specs/matches")
    debug_cmd.add_argument("--db", help="SQLite DB path override")

    return parser


def main(argv: Iterable[str] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    mappings_path = Path(args.mappings).resolve() if hasattr(args, "mappings") else None

    if args.command == "validate-mappings":
        config = load_config(mock_mode=False)
        return validate_mappings(config, mappings_path)

    if args.command == "discover":
        from arbscan.suggest import write_suggestions

        config = load_config(mock_mode=False)
        write_suggestions(config, Path(args.output).resolve(), limit=args.limit)
        return 0

    if args.command == "kalshi-search":
        from arbscan.cli_kalshi import kalshi_search

        config = load_config(mock_mode=False)
        kalshi_search(
            config=config,
            query=args.query,
            limit=args.limit,
            status=args.status,
            series_ticker=args.series_ticker,
            close_after=args.close_after,
            close_before=args.close_before,
            out_path=args.out,
        )
        return 0

    if args.command == "kalshi-series":
        from arbscan.cli_kalshi import kalshi_series

        config = load_config(mock_mode=False)
        kalshi_series(config=config, series_ticker=args.ticker)
        return 0

    if args.command == "polymarket-search":
        from arbscan.cli_polymarket import polymarket_search

        config = load_config(mock_mode=False)
        polymarket_search(config=config, query=args.query, limit=args.limit, out_path=args.out)
        return 0

    if args.command == "generate-mappings":
        from arbscan.generate_mappings import generate_mappings

        generate_mappings(args.kalshi, args.polymarket, args.out)
        return 0

    if args.command == "scan-catalog":
        from arbscan.catalog import scan_catalog

        config = load_config(mock_mode=False)
        if hasattr(args, "db") and args.db:
            config = replace(config, db_path=args.db)
        scan_catalog(config, sports_only=bool(args.sports_only))
        return 0

    if args.command == "run-live":
        from arbscan.live import run_live

        config = load_config(mock_mode=False)
        if hasattr(args, "db") and args.db:
            config = replace(config, db_path=args.db)
        asyncio.run(run_live(config, sports_only=bool(args.sports_only)))
        return 0

    if args.command == "review":
        from arbscan.review import export_review

        config = load_config(mock_mode=False)
        if hasattr(args, "db") and args.db:
            config = replace(config, db_path=args.db)
        export_review(config, args.min_score, args.out)
        return 0

    if args.command == "explain-event-match":
        from arbscan.review import explain_event_match

        config = load_config(mock_mode=False)
        explain_event_match(config, args.kalshi_event, args.polymarket_event)
        return 0

    if args.command == "explain-outcome-match":
        from arbscan.review import explain_outcome_match

        config = load_config(mock_mode=False)
        explain_outcome_match(config, args.kalshi_market, args.polymarket_market)
        return 0

    if args.command == "debug-fed-decision":
        from arbscan.debug_fed_decision import run_debug

        config = load_config(mock_mode=False)
        run_debug(config)
        return 0

    if args.command == "debug-sports-match":
        from arbscan.debug_sports_match import run_debug

        config = load_config(mock_mode=False)
        run_debug(config, args.kalshi_market, args.polymarket_market)
        return 0

    if args.command == "validate-fed-example":
        from arbscan.validate_fed_example import run_validate

        config = load_config(mock_mode=False)
        run_validate(config)
        return 0

    if args.command == "debug-db":
        from arbscan.debug_db import debug_db

        config = load_config(mock_mode=False)
        if hasattr(args, "db") and args.db:
            config = replace(config, db_path=args.db)
        debug_db(config)
        return 0

    if args.command == "matcher-stats":
        from arbscan.matcher_stats import matcher_stats

        config = load_config(mock_mode=False)
        if hasattr(args, "db") and args.db:
            config = replace(config, db_path=args.db)
        matcher_stats(config)
        return 0

    config = load_config(mock_mode=args.mock)
    asyncio.run(run_scanner(config, mappings_path))
    return 0
