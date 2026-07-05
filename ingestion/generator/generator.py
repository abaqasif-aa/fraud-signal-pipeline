"""
Transaction Event Generator
=============================
Generates synthetic credit card transaction events that mimic
real-world fraud distribution patterns.

Each event is self-contained — category, fraud status, and amount
are all decided together in a single pass (not in separate steps).

Key design decisions:
  - Weighted category distribution mirrors real transaction volumes
  - Fraud rate is elevated for high-risk countries (3x baseline)
  - Fraud amounts are bimodal: either very high OR micro-probe ($0.50-$5)
  - Legitimate amounts follow log-normal distribution (mirrors real spending)
"""

import argparse
import json
import logging
import math
import random
import time
import uuid
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Category distribution ───────────────────────────────────
# weight      : fraction of all transactions (must sum to 1.0)
# normal_mean : average legitimate transaction amount in USD
# fraud_mean  : average fraudulent transaction amount in USD
# Fraud amounts are higher because stolen cards get maxed out quickly
MERCHANT_CATEGORIES = {
    "grocery":        {"weight": 0.22, "normal_mean": 65,   "fraud_mean": 120},
    "retail":         {"weight": 0.18, "normal_mean": 95,   "fraud_mean": 280},
    "restaurant":     {"weight": 0.15, "normal_mean": 38,   "fraud_mean": 95},
    "electronics":    {"weight": 0.10, "normal_mean": 320,  "fraud_mean": 890},
    "travel":         {"weight": 0.09, "normal_mean": 580,  "fraud_mean": 1200},
    "fuel":           {"weight": 0.08, "normal_mean": 55,   "fraud_mean": 110},
    "entertainment":  {"weight": 0.07, "normal_mean": 45,   "fraud_mean": 180},
    "healthcare":     {"weight": 0.05, "normal_mean": 140,  "fraud_mean": 400},
    "subscription":   {"weight": 0.04, "normal_mean": 14,   "fraud_mean": 14},   # fraud_mean same — not a typical fraud target
    "atm_withdrawal": {"weight": 0.02, "normal_mean": 200,  "fraud_mean": 500},
}

# ── Country distribution ────────────────────────────────────
# Weights represent share of global card transaction volume
COUNTRIES = {
    "US": 0.45, "GB": 0.10, "CA": 0.08, "AU": 0.06,
    "DE": 0.06, "FR": 0.05, "JP": 0.04, "BR": 0.03,
    "IN": 0.03, "MX": 0.02, "NG": 0.02, "RU": 0.02,
    "CN": 0.02, "UA": 0.02,
}

# Countries with elevated fraud probability
# Transactions from these get 3x the base fraud rate
HIGH_RISK_COUNTRIES = {"NG", "RU", "UA", "BR"}

# ── Other distributions ─────────────────────────────────────
CARD_TYPES   = {"credit": 0.55, "debit": 0.38, "prepaid": 0.07}
DEVICE_TYPES = {"mobile": 0.40, "desktop": 0.30, "pos_terminal": 0.20, "tablet": 0.07, "atm": 0.03}
CURRENCIES   = {"USD": 0.60, "EUR": 0.20, "GBP": 0.10, "CAD": 0.06, "AUD": 0.04}

# ── Fraud amount behaviour ──────────────────────────────────
# 70% of fraud = high value (max out the card)
# 30% of fraud = micro probe ($0.50-$5 to test if card is active)
FRAUD_PROBE_RATE = 0.30


def weighted_choice(distribution: dict) -> str:
    """
    Pick a key from a weighted dictionary.
    Works for both formats we use:
      - values are dicts with a 'weight' key (MERCHANT_CATEGORIES)
      - values are plain floats (COUNTRIES, CURRENCIES etc.)
    """
    keys    = list(distribution.keys())
    weights = [
        v["weight"] if isinstance(v, dict) else v
        for v in distribution.values()
    ]
    return random.choices(keys, weights=weights, k=1)[0]


def generate_amount(category: str, is_fraud: bool) -> float:
    """
    Generate a transaction amount based on category and fraud status.

    Legitimate amounts: log-normal distribution centred on normal_mean.
    Log-normal is used because real spending has a long right tail
    (most purchases are small, occasional large ones exist).

    Fraud amounts are bimodal:
      - 70%: high value (0.8x to 3x fraud_mean) — stolen card being maxed out
      - 30%: micro probe ($0.50-$5) — testing if card is active before big purchase
    """
    dist = MERCHANT_CATEGORIES[category]

    # Pick which mean to use based on fraud status
    mean = dist["fraud_mean"] if is_fraud else dist["normal_mean"]

    if is_fraud:
        if random.random() > FRAUD_PROBE_RATE:
            # High value fraud — uniform between 80% and 300% of fraud mean
            amount = random.uniform(mean * 0.8, mean * 3.0)
        else:
            # Micro probe — tiny charge to test card validity
            amount = random.uniform(0.50, 5.00)
    else:
        # Legitimate — log-normal clusters around mean with realistic spread
        amount = random.lognormvariate(math.log(mean), 0.5)

    # Safety bounds matching data contract: min $0.01, max $50,000
    return round(max(0.01, min(amount, 50000.0)), 2)


def generate_event(
    user_pool_size: int = 10000,
    merchant_pool_size: int = 500,
    fraud_rate: float = 0.02,
) -> dict:
    """
    Generate a single complete transaction event.

    All fields are decided in one pass — category, fraud status,
    and amount are causally linked, not assigned separately.

    Args:
        user_pool_size     : number of unique users to simulate
        merchant_pool_size : number of unique merchants to simulate
        fraud_rate         : baseline fraud probability (default 2%)

    Returns:
        dict with all transaction fields + _is_fraud_label for ML training
    """
    # Step 1: pick category (drives amount distribution)
    category = weighted_choice(MERCHANT_CATEGORIES)

    # Step 2: pick country (drives fraud rate adjustment)
    country = weighted_choice(COUNTRIES)

    # Step 3: decide fraud status
    # High risk countries get 3x baseline, capped at 20%
    effective_fraud_rate = (
        min(fraud_rate * 3, 0.20)
        if country in HIGH_RISK_COUNTRIES
        else fraud_rate
    )
    is_fraud = random.random() < effective_fraud_rate

    # Step 4: generate amount — knows fraud status
    amount = generate_amount(category, is_fraud)

    # Step 5: online vs in-person — fraud is more likely online
    # because card-not-present fraud doesn't require physical card
    is_online = random.random() < (0.70 if is_fraud else 0.45)

    # Step 6: device type — only meaningful for online transactions
    device_type = (
        weighted_choice(DEVICE_TYPES)
        if is_online
        else "pos_terminal"   # always a physical terminal for in-person
    )

    # Step 7: IP address — only exists for online transactions
    # Masked to /24 subnet (last octet zeroed) for privacy
    ip_address = (
        f"{random.randint(1,254)}.{random.randint(0,255)}.{random.randint(0,255)}.0"
        if is_online
        else None
    )

    return {
        "event_id":           str(uuid.uuid4()),          # UUID v4 — unique per event
        "event_timestamp":    datetime.now(timezone.utc).isoformat(),
        "user_id":            f"USR_{random.randint(1, user_pool_size):06d}",
        "merchant_id":        f"MER_{random.randint(1, merchant_pool_size):05d}",
        "merchant_category":  category,
        "amount":             amount,
        "currency":           weighted_choice(CURRENCIES),
        "country_code":       country,
        "card_type":          weighted_choice(CARD_TYPES),
        "is_online":          is_online,
        "device_type":        device_type,
        "ip_address":         ip_address,
        "_is_fraud_label":    is_fraud,   # training label — stripped before Firehose
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Synthetic transaction event generator")
    parser.add_argument("--rate",       type=int,   default=5,    help="Events per second")
    parser.add_argument("--fraud-rate", type=float, default=0.02, help="Baseline fraud probability")
    parser.add_argument("--duration",   type=int,   default=30,   help="Run for N seconds")
    args = parser.parse_args()

    log.info(
        f"Generator starting | "
        f"rate={args.rate}/s | "
        f"fraud={args.fraud_rate:.1%} | "
        f"duration={args.duration}s"
    )

    start   = time.time()
    emitted = 0
    fraud   = 0

    while time.time() - start < args.duration:
        event = generate_event(fraud_rate=args.fraud_rate)
        print(json.dumps(event, indent=2))
        emitted += 1
        if event["_is_fraud_label"]:
            fraud += 1
        time.sleep(1.0 / args.rate)

    log.info(
        f"Done | "
        f"emitted={emitted:,} | "
        f"fraud={fraud} ({100*fraud/max(emitted,1):.1f}%)"
    )
