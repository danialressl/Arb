import json

from arbscan.canonical import predicate_from_dict, predicate_key
from arbscan.config import AppConfig
from arbscan.storage import list_market_matches, list_markets_for_event


def run_validate(config: AppConfig) -> None:
    kalshi_event_id = "KXFEDDECISION-26JAN"
    poly_event_id = "fed-decision-in-january"

    kalshi_markets = list_markets_for_event(config.db_path, "kalshi", kalshi_event_id)
    poly_markets = list_markets_for_event(config.db_path, "polymarket", poly_event_id)

    if not kalshi_markets or not poly_markets:
        print("Missing persisted markets. Run scan-catalog first.")
        return

    kalshi_preds = _predicate_map(kalshi_markets)
    poly_preds = _predicate_map(poly_markets)

    print("market_label | venue | canonical_predicate")
    for label, predicate in kalshi_preds.values():
        print(f"{label} | kalshi | {predicate}")
    for label, predicate in poly_preds.values():
        print(f"{label} | polymarket | {predicate}")

    matches = []
    for key, (k_label, k_pred) in kalshi_preds.items():
        if key in poly_preds:
            p_label, p_pred = poly_preds[key]
            matches.append((k_label, p_label, k_pred, p_pred))

    print("matched pairs")
    for k_label, p_label, k_pred, _ in matches:
        print(f"{k_label} <-> {p_label} | {k_pred}")

    match_keys = {key for key in kalshi_preds if key in poly_preds}
    needed = {
        "delta_bps|==|0|bps",
        "delta_bps|==|-25|bps",
        "delta_bps|>=|25|bps",
    }
    missing = needed - match_keys
    if missing:
        raise SystemExit(f"Missing expected matches: {sorted(missing)}")

    forbidden = "delta_bps|<=|-50|bps"
    if forbidden in match_keys:
        raise SystemExit("Unexpected CUT_50_PLUS match present.")

    all_matches = list_market_matches(config.db_path, active_only=False)
    if not all_matches:
        raise SystemExit("No market_matches rows found.")
    kalshi_ids = {row["market_id"] for row in kalshi_markets}
    poly_ids = {row["market_id"] for row in poly_markets}
    matched_keys = set()
    for row in all_matches:
        if row["kalshi_market_id"] in kalshi_ids and row["polymarket_market_id"] in poly_ids:
            predicate = predicate_from_dict(json.loads(row["canonical_predicate"] or "null"))
            if predicate:
                matched_keys.add(predicate_key(predicate))
    if not needed.issubset(matched_keys):
        raise SystemExit(f"Expected matches missing in market_matches: {sorted(needed - matched_keys)}")
    if forbidden in matched_keys:
        raise SystemExit("Unexpected CUT_50_PLUS match persisted.")


def _predicate_map(markets: list) -> dict:
    predicates = {}
    for market in markets:
        raw = market.get("canonical_predicate")
        if not raw:
            continue
        predicate = predicate_from_dict(json.loads(raw))
        if not predicate:
            continue
        key = predicate_key(predicate)
        predicates[key] = (market.get("title") or market.get("market_id") or "", raw)
    return predicates
