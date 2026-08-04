"""
Vectorized synthetic event generator for upbase.events, built entirely in
Polars (no per-row Python loops). Designed to produce ~1,000,000 rows that
look like real product-analytics data:

  - Each user signs up exactly once, at a random second within the given
    [earliest, latest] window.
  - Row count per user follows a Pareto-shaped distribution: a small
    fraction of users generate a disproportionate share of events/LTV
    (the "80/20" pattern), rather than every user getting an equal count.
  - Each user's events after signup arrive via exponential inter-arrival
    gaps, cumulatively summed per user — activity clusters early and
    tapers off. Users whose cumulative time overshoots the window simply
    end up with fewer rows (natural churn), no special-casing needed.
  - device / country / referrer are assigned ONCE per user and broadcast
    to all of that user's rows (a real user doesn't switch device every
    event).
  - event_type is a numbered label (1_signup ... 7_logout) so funnel-style
    charts sort correctly without extra config.
  - revenue is log-normal on purchase rows only, matching real transaction
    amount distributions (mostly small purchases, a long tail of big ones).

Output is a Polars DataFrame, convertible directly to Arrow for a fast
bulk insert into ClickHouse via clickhouse-connect's insert_arrow.
"""

from datetime import datetime, timedelta
import numpy as np
import polars as pl

import clickhouse_connect
from airflow import DAG 
from airflow.models.param import Param
from airflow.providers.standard.operators.python import PythonOperator

EVENT_TYPES = [
    "1_signup",
    "2_login",
    "3_view_page",
    "4_click",
    "5_add_to_cart",
    "6_purchase",
    "7_logout",
]
# Weights apply to POST-signup events only (signup itself is injected
# separately as each user's first row).
POST_SIGNUP_EVENT_TYPES = EVENT_TYPES[1:]
POST_SIGNUP_EVENT_WEIGHTS = [12, 40, 25, 10, 5, 8]

DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [55, 35, 10]

COUNTRIES = ["US", "IN", "BR", "GB", "DE", "ID", "NG", "PH", "VN", "FR"]
COUNTRY_WEIGHTS = [30, 15, 8, 7, 6, 6, 5, 5, 4, 4]

REFERRERS = ["organic_search", "paid_ad", "social", "direct", "email", "referral"]
REFERRER_WEIGHTS = [30, 20, 20, 15, 10, 5]

REVENUE_LOGNORMAL_MEAN = 3.0   # parameters of the underlying normal dist,
REVENUE_LOGNORMAL_SIGMA = 0.8  # not the dollar values directly


def clear_table():
    client = clickhouse_connect.get_client(
        host='clickhouse', port=8123,
        username='default', password='upbase123',
        database='upbase'
    )
    client.command("TRUNCATE TABLE events")

def _weighted_random_per_user(num_users: int, options: list, weights: list, rng: np.random.Generator):
    probs = np.array(weights, dtype=float)
    probs /= probs.sum()
    return rng.choice(options, size=num_users, p=probs)


def _pareto_row_counts(num_users: int, total_rows: int, rng: np.random.Generator) -> np.ndarray:
    """
    Draws a Pareto-shaped 'weight' per user, then scales those weights so
    they sum to total_rows (with integer row counts, each user >= 1).
    Produces the 80/20-style skew: a minority of users account for a
    majority of total events.
    """
    raw = rng.pareto(a=1.5, size=num_users) + 1.0  # shape param controls skew
    scaled = raw / raw.sum() * total_rows
    counts = np.maximum(np.round(scaled).astype(int), 1)

    # Correct any rounding drift so the total is exactly total_rows
    diff = total_rows - counts.sum()
    if diff != 0:
        idx = rng.choice(num_users, size=abs(diff), replace=True)
        np.add.at(counts, idx, 1 if diff > 0 else -1)
        counts = np.maximum(counts, 1)

    return counts


def generate_realistic_events(
    total_rows: int = 1_000_000,
    num_users: int = 20_000,
    earliest: datetime = datetime(2026, 1, 1),
    latest: datetime = datetime(2026, 7, 1),
    seed: int | None = 42,
) -> pl.DataFrame:
    rng = np.random.default_rng(seed)

    earliest_ts = int(earliest.timestamp())
    latest_ts = int(latest.timestamp())

    # ---- 1. Row count per user (Pareto-weighted), then expand to row-level user_id ----
    rows_per_user = _pareto_row_counts(num_users, total_rows, rng)
    user_ids = np.repeat(np.arange(num_users), rows_per_user)

    # ---- 2. Per-user signup time, second precision, broadcast to rows ----
    signup_per_user = rng.integers(earliest_ts, latest_ts + 1, size=num_users)
    signup_col = signup_per_user[user_ids]

    # ---- 3. Per-user device/country/referrer, broadcast to rows ----
    device_per_user = _weighted_random_per_user(num_users, DEVICES, DEVICE_WEIGHTS, rng)
    country_per_user = _weighted_random_per_user(num_users, COUNTRIES, COUNTRY_WEIGHTS, rng)
    referrer_per_user = _weighted_random_per_user(num_users, REFERRERS, REFERRER_WEIGHTS, rng)

    device_col = device_per_user[user_ids]
    country_col = country_per_user[user_ids]
    referrer_col = referrer_per_user[user_ids]

    # ---- 4. Inter-arrival gaps (seconds) per row, then cumulative offset per user ----
    # Scale controls how "bursty vs spread out" activity is; larger scale = slower decay.
    gap_scale_per_user = rng.uniform(3_600, 6 * 3_600, size=num_users)  # 1-6 hr avg gap
    gap_scale_col = gap_scale_per_user[user_ids]
    gaps = rng.exponential(scale=gap_scale_col)

    df = pl.DataFrame({
        "user_id": user_ids,
        "signup_ts": signup_col,
        "device": device_col,
        "country": country_col,
        "referrer": referrer_col,
        "gap": gaps,
    })

    df = df.with_columns([
        pl.col("gap").cum_sum().over("user_id").alias("offset"),
        (pl.col("gap").cum_count().over("user_id") == 1).alias("is_first"),
    ])
    df = df.with_columns(
        pl.when(pl.col("is_first")).then(0.0).otherwise(pl.col("offset")).alias("offset")
    )

    # ---- 5. Final event_time = signup + offset, clip anything past the window (churn) ----
    df = df.with_columns(
        (pl.col("signup_ts") + pl.col("offset")).alias("event_ts")
    )
    df = df.filter(pl.col("event_ts") <= latest_ts)

    # ---- 6. event_type: signup on the first row per user, weighted random otherwise ----
    post_signup_types = rng.choice(
        POST_SIGNUP_EVENT_TYPES,
        size=df.height,
        p=np.array(POST_SIGNUP_EVENT_WEIGHTS) / sum(POST_SIGNUP_EVENT_WEIGHTS),
    )
    df = df.with_columns(pl.Series("event_type_candidate", post_signup_types))
    df = df.with_columns(
        pl.when(pl.col("is_first"))
        .then(pl.lit(EVENT_TYPES[0]))
        .otherwise(pl.col("event_type_candidate"))
        .alias("event_type")
    )

    # ---- 7. revenue: log-normal on purchase rows only, 0 elsewhere ----
    revenue_draws = rng.lognormal(mean=REVENUE_LOGNORMAL_MEAN, sigma=REVENUE_LOGNORMAL_SIGMA, size=df.height)
    df = df.with_columns(pl.Series("revenue_draw", revenue_draws))
    df = df.with_columns(
        pl.when(pl.col("event_type") == "6_purchase")
        .then(pl.col("revenue_draw").round(2))
        .otherwise(0.0)
        .alias("revenue")
    )

    # ---- 8. Finalize columns, convert timestamp, sort ----
    df = df.with_columns(
        pl.from_epoch(pl.col("event_ts"), time_unit="s").alias("event_time")
    )

    result = df.select([
        "event_time",
        "user_id",
        "event_type",
        "device",
        "country",
        "referrer",
        "revenue",
    ]).sort("event_time")

    return result

# ---- Wrapper ----
def generate_wrapper(**context):
    num_rows = context["params"]["num_rows"]
    num_users = context["params"]["num_users"]
    events_df = generate_realistic_events(total_rows=num_rows, num_users=num_users)
    client = clickhouse_connect.get_client(
        host='clickhouse', port=8123,
        username='default', password='upbase123',
        database='upbase'
    )
    client.insert_arrow("events", events_df.to_arrow())


# ---- Push ----
default_args = {
    'owner': 'upbase',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='main_generation',
    default_args=default_args,
    start_date=datetime(2026, 1, 1), 
    schedule=None,
    catchup=False,
    params={"num_rows": Param(1_500_000, type="integer", minimum=1_000_000, maximum=1_750_000
                              , description="Baseline number of rows to be generated")
            , "num_users": Param(16_384, type="integer", minimum=8_192, maximum=65_536
                              , description="Number of users to be generated")},
) as dag:
    clear_task = PythonOperator(
        task_id='clear_events_table',
        python_callable=clear_table,
    )   
    generate_task = PythonOperator(
        task_id='generate_fresh_events',
        python_callable=generate_wrapper,
    )   

    clear_task >> generate_task


# ---- Debug ----
if __name__ == "__main__":
    print(events_df.shape)
    print(events_df.head(10))
    print(events_df.group_by("event_type").len().sort("event_type"))

