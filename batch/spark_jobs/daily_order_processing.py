"""
Daily Order Processing — PySpark Batch Job

Business problem this solves:
Streaming consumers (Session 2) handle real-time events.
But some analytics cannot be computed in real time:
- 30-day cohort retention requires historical data
- Seller reputation needs weeks of order history
- Fraud pattern analysis needs aggregate view

This job runs nightly at 2am.
Processes all orders from the last 30 days.
Writes results to GCS and BigQuery for dashboards.

Why PySpark not pandas:
30 days of Meesho orders = potentially 15 million rows.
pandas loads everything into one machine's memory = crash.
PySpark distributes across all CPU cores = handles any size.
Same code runs locally and on a 100-node cluster.

Architecture:
Raw Kafka events → GCS Parquet files → THIS JOB → BigQuery marts
"""

import sys
import os
import logging
from datetime import datetime, timedelta, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from batch.utils.spark_session import create_spark_session
from batch.utils.data_quality import run_all_checks

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def generate_meesho_data(spark: SparkSession) -> DataFrame:
    """
    Generate realistic Meesho order data for batch processing.

    In production: reads from GCS Parquet files written by Kafka archiver.
    Here: generates synthetic data for demonstration.

    Why synthetic data mirrors production:
    Real distributions (tier2/tier3 bias, UPI 45%, Women Fashion 35%)
    make downstream analytics meaningful.
    Random data produces meaningless results.
    """
    import random
    from ingestion.event_generator import MeeshoEventGenerator

    logger.info("Generating 30 days of synthetic order data...")

    gen = MeeshoEventGenerator(
        num_sellers=50,
        num_customers=500,
        num_products=200
    )

    all_orders = []
    base_date = datetime.now(timezone.utc) - timedelta(days=30)

    for day in range(30):
        current_date = base_date + timedelta(days=day)
        is_sale = day in [7, 14, 21, 28]  # Weekly sale days

        gen.is_sale_day = is_sale
        daily_orders = 200 if is_sale else 50

        for _ in range(daily_orders):
            order = gen.generate_order_event()
            order_dict = order.to_dict()
            order_dict['order_date'] = current_date.date().isoformat()
            order_dict['order_hour'] = random.randint(8, 23)
            order_dict['is_sale_day'] = is_sale
            all_orders.append(order_dict)

    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("seller_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("subcategory", StringType(), True),
        StructField("amount_inr", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("payment_method", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("city_tier", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("order_hour", IntegerType(), True),
        StructField("is_sale_day", BooleanType(), True),
        StructField("device_type", StringType(), True),
    ])

    df = spark.createDataFrame(all_orders, schema) \
        .withColumn("order_date", F.to_date("order_date"))

    count = df.count()
    logger.info(f"Generated {count:,} orders across 30 days")
    return df


# ════════════════════════════════════════════
# TRANSFORMATION 1 — Daily Revenue Summary
# ════════════════════════════════════════════

def compute_daily_revenue(orders_df: DataFrame) -> DataFrame:
    """
    Daily revenue breakdown by category and city tier.

    Business value:
    Leadership reviews this every morning.
    Did yesterday hit revenue targets?
    Which categories over/underperformed?
    Which city tiers drove growth?

    PySpark concepts used:
    - groupBy with multiple columns
    - Multiple aggregations in one pass
    - withColumn for derived metrics
    - orderBy for readable output
    """
    logger.info("Computing daily revenue summary...")

    daily = orders_df.groupBy(
        'order_date',
        'category',
        'city_tier',
        'is_sale_day'
    ).agg(
        F.sum('amount_inr').alias('total_revenue'),
        F.count('order_id').alias('order_count'),
        F.avg('amount_inr').alias('avg_order_value'),
        F.countDistinct('customer_id').alias('unique_customers'),
        F.countDistinct('seller_id').alias('active_sellers'),
        F.sum(F.when(F.col('payment_method') == 'UPI',
                     F.col('amount_inr')).otherwise(0)
              ).alias('upi_revenue'),
    ).withColumn(
        'upi_share_pct',
        F.round(F.col('upi_revenue') / F.col('total_revenue') * 100, 2)
    ).withColumn(
        'revenue_per_customer',
        F.round(F.col('total_revenue') / F.col('unique_customers'), 2)
    ).orderBy('order_date', F.desc('total_revenue'))

    logger.info(f"Daily revenue: {daily.count()} rows")
    return daily


# ════════════════════════════════════════════
# TRANSFORMATION 2 — Cohort Retention Analysis
# ════════════════════════════════════════════

def compute_cohort_retention(orders_df: DataFrame) -> DataFrame:
    """
    Week-over-week customer cohort retention.

    Business value:
    Cohort retention answers: of customers who joined in week 1,
    how many came back in week 2, week 3, week 4?
    Declining retention = product problem.
    Flat retention = healthy engagement.

    This is your mart_user_retention logic from Project 1
    but implemented in PySpark for scale.

    PySpark concepts used:
    - Window function for first order date per customer
    - Date arithmetic with datediff
    - GroupBy on computed cohort columns
    - Conditional aggregation
    """
    logger.info("Computing cohort retention...")

    # Step 1: Find each customer's first order date (cohort assignment)
    # Window function: MIN(order_date) per customer
    customer_window = Window.partitionBy('customer_id')

    with_cohort = orders_df \
        .withColumn(
            'cohort_date',
            F.min('order_date').over(customer_window)
        ) \
        .withColumn(
            'cohort_week',
            F.date_trunc('week', F.col('cohort_date'))
        ) \
        .withColumn(
            'activity_week',
            F.date_trunc('week', F.col('order_date'))
        ) \
        .withColumn(
            'weeks_since_signup',
            (F.datediff(
                F.col('activity_week'),
                F.col('cohort_week')
            ) / 7).cast(IntegerType())
        )

    # Step 2: Count cohort size (users per cohort week)
    cohort_sizes = with_cohort \
        .filter(F.col('weeks_since_signup') == 0) \
        .groupBy('cohort_week') \
        .agg(F.countDistinct('customer_id').alias('cohort_size'))

    # Step 3: Count retained users per cohort per week
    retention = with_cohort \
        .groupBy('cohort_week', 'weeks_since_signup') \
        .agg(
            F.countDistinct('customer_id').alias('retained_users'),
            F.sum('amount_inr').alias('cohort_revenue')
        )

    # Step 4: Join cohort size and compute retention rate
    result = retention \
        .join(cohort_sizes, on='cohort_week', how='left') \
        .withColumn(
            'retention_rate_pct',
            F.round(
                F.col('retained_users') / F.col('cohort_size') * 100,
                2
            )
        ) \
        .orderBy('cohort_week', 'weeks_since_signup')

    logger.info(f"Cohort retention: {result.count()} rows")
    return result


# ════════════════════════════════════════════
# TRANSFORMATION 3 — Seller Performance Score
# ════════════════════════════════════════════

def compute_seller_performance(orders_df: DataFrame) -> DataFrame:
    """
    Seller reputation and performance scoring.

    Business value:
    Meesho ranks sellers on the platform.
    High-performing sellers get more visibility.
    Low performers get less. Fraud sellers get flagged.
    This job computes the score that drives those decisions.

    Scoring formula:
    - Revenue contribution (40%)
    - Order volume (20%)
    - Geographic reach (15%)
    - Customer diversity (15%)
    - Category consistency (10%)

    PySpark concepts used:
    - Multiple window functions
    - Percent rank for normalization
    - Weighted score calculation
    - Dense rank for leaderboard
    """
    logger.info("Computing seller performance scores...")

    # Base metrics per seller
    seller_metrics = orders_df.groupBy('seller_id') \
        .agg(
            F.sum('amount_inr').alias('total_revenue'),
            F.count('order_id').alias('total_orders'),
            F.avg('amount_inr').alias('avg_order_value'),
            F.countDistinct('customer_id').alias('unique_customers'),
            F.countDistinct('city').alias('cities_reached'),
            F.countDistinct('category').alias('categories_sold'),
            F.countDistinct('order_date').alias('active_days'),
            F.sum(F.when(
                F.col('city_tier') == 'tier2', 1).otherwise(0)
            ).alias('tier2_orders'),
            F.sum(F.when(
                F.col('city_tier') == 'tier3', 1).otherwise(0)
            ).alias('tier3_orders'),
        ) \
        .withColumn(
            'tier2_tier3_share',
            F.round(
                (F.col('tier2_orders') + F.col('tier3_orders')) /
                F.col('total_orders') * 100, 2
            )
        ) \
        .withColumn(
            'revenue_per_active_day',
            F.round(F.col('total_revenue') / F.col('active_days'), 2)
        )

    # Normalize each metric using percent_rank (0 to 1)
    # Higher percentile = better performance
    rank_window = Window.orderBy(F.col('total_revenue'))

    normalized = seller_metrics \
        .withColumn(
            'revenue_percentile',
            F.percent_rank().over(Window.orderBy('total_revenue'))
        ) \
        .withColumn(
            'volume_percentile',
            F.percent_rank().over(Window.orderBy('total_orders'))
        ) \
        .withColumn(
            'reach_percentile',
            F.percent_rank().over(Window.orderBy('cities_reached'))
        ) \
        .withColumn(
            'diversity_percentile',
            F.percent_rank().over(Window.orderBy('unique_customers'))
        ) \
        .withColumn(
            'tier_reach_percentile',
            F.percent_rank().over(Window.orderBy('tier2_tier3_share'))
        )

    # Weighted score: 40% revenue + 20% volume + 15% reach + 15% diversity + 10% tier
    scored = normalized \
        .withColumn(
            'performance_score',
            F.round(
                F.col('revenue_percentile') * 0.40 +
                F.col('volume_percentile') * 0.20 +
                F.col('reach_percentile') * 0.15 +
                F.col('diversity_percentile') * 0.15 +
                F.col('tier_reach_percentile') * 0.10,
                4
            )
        ) \
        .withColumn(
            'seller_tier',
            F.when(F.col('performance_score') >= 0.8, 'PLATINUM')
             .when(F.col('performance_score') >= 0.6, 'GOLD')
             .when(F.col('performance_score') >= 0.4, 'SILVER')
             .otherwise('BRONZE')
        ) \
        .withColumn(
            'rank',
            F.dense_rank().over(
                Window.orderBy(F.desc('performance_score'))
            )
        ) \
        .orderBy('rank')

    logger.info(f"Seller performance: {scored.count()} sellers scored")
    return scored


# ════════════════════════════════════════════
# TRANSFORMATION 4 — Fraud Pattern Analysis
# ════════════════════════════════════════════

def detect_batch_fraud_patterns(orders_df: DataFrame) -> DataFrame:
    """
    Historical fraud pattern detection.

    Real-time (Session 2): catches velocity fraud in seconds.
    Batch (this job): catches sophisticated patterns that
    require historical context — impossible in real-time.

    Patterns detected:
    1. Seller-customer collusion: same seller-customer pair
       appears unusually often
    2. Geographic impossibility: orders from distant cities
       within minutes (impossible for one person)
    3. Round-number orders: fraudsters often use exact amounts
    4. Timing patterns: orders only at off-hours = bot behavior

    PySpark concepts used:
    - Self-join for pair analysis
    - Window functions for temporal analysis
    - Complex conditional logic with when/otherwise
    - Multiple fraud signals combined
    """
    logger.info("Detecting batch fraud patterns...")

    # Signal 1: seller-customer pair frequency
    # If same customer buys from same seller 5+ times = suspicious
    pair_counts = orders_df \
        .groupBy('seller_id', 'customer_id') \
        .agg(
            F.count('order_id').alias('pair_order_count'),
            F.sum('amount_inr').alias('pair_revenue'),
            F.countDistinct('order_date').alias('pair_active_days')
        ) \
        .withColumn(
            'collusion_signal',
            F.when(F.col('pair_order_count') > 10, True)
             .otherwise(False)
        )

    # Signal 2: round number orders per seller
    # Real orders rarely end in exactly .00
    round_number = orders_df \
        .withColumn(
            'is_round_number',
            (F.col('amount_inr') % 100 == 0).cast(IntegerType())
        ) \
        .groupBy('seller_id') \
        .agg(
            F.sum('is_round_number').alias('round_number_orders'),
            F.count('order_id').alias('total_orders')
        ) \
        .withColumn(
            'round_number_rate',
            F.round(
                F.col('round_number_orders') / F.col('total_orders'),
                4
            )
        ) \
        .withColumn(
            'round_number_signal',
            F.when(F.col('round_number_rate') > 0.5, True)
             .otherwise(False)
        )

    # Signal 3: off-hours ordering pattern
    # Bot orders typically cluster at specific hours
    hourly = orders_df \
        .withColumn(
            'is_off_hours',
            F.when(
                (F.col('order_hour') < 7) | (F.col('order_hour') > 23),
                1
            ).otherwise(0)
        ) \
        .groupBy('seller_id') \
        .agg(
            F.sum('is_off_hours').alias('off_hours_orders'),
            F.count('order_id').alias('total_orders'),
            F.collect_set('order_hour').alias('active_hours')
        ) \
        .withColumn(
            'off_hours_rate',
            F.round(
                F.col('off_hours_orders') / F.col('total_orders'),
                4
            )
        ) \
        .withColumn(
            'timing_signal',
            F.when(F.col('off_hours_rate') > 0.3, True)
             .otherwise(False)
        )

    # Combine signals into fraud risk score
    fraud_signals = round_number \
        .join(
            hourly.select(
                'seller_id', 'off_hours_rate', 'timing_signal'
            ),
            on='seller_id',
            how='left'
        ) \
        .withColumn(
            'fraud_signal_count',
            F.col('round_number_signal').cast(IntegerType()) +
            F.col('timing_signal').cast(IntegerType())
        ) \
        .withColumn(
            'fraud_risk_level',
            F.when(F.col('fraud_signal_count') >= 2, 'HIGH')
             .when(F.col('fraud_signal_count') == 1, 'MEDIUM')
             .otherwise('LOW')
        ) \
        .orderBy(F.desc('fraud_signal_count'), F.desc('round_number_rate'))

    high_risk = fraud_signals.filter(
        F.col('fraud_risk_level') == 'HIGH'
    ).count()

    logger.info(
        f"Fraud analysis: {fraud_signals.count()} sellers analyzed, "
        f"{high_risk} HIGH risk"
    )
    return fraud_signals


# ════════════════════════════════════════════
# TRANSFORMATION 5 — Category Trend Analysis
# ════════════════════════════════════════════

def compute_category_trends(orders_df: DataFrame) -> DataFrame:
    """
    Week-over-week category revenue trends.

    Business value:
    Which categories are growing? Which are declining?
    Marketing team uses this to allocate ad spend.
    Women Fashion growing 20% WoW = increase fashion ads.
    Electronics declining = investigate and fix.

    PySpark concepts used:
    - Date truncation for weekly grouping
    - LAG window function for previous week comparison
    - Percentage change calculation
    - Null handling with coalesce
    """
    logger.info("Computing category trends...")

    # Weekly revenue per category
    weekly = orders_df \
        .withColumn(
            'order_week',
            F.date_trunc('week', F.col('order_date'))
        ) \
        .groupBy('order_week', 'category') \
        .agg(
            F.sum('amount_inr').alias('weekly_revenue'),
            F.count('order_id').alias('weekly_orders'),
            F.countDistinct('customer_id').alias('weekly_customers')
        )

    # LAG to get previous week's revenue
    category_time_window = Window \
        .partitionBy('category') \
        .orderBy('order_week')

    with_trends = weekly \
        .withColumn(
            'prev_week_revenue',
            F.lag('weekly_revenue', 1).over(category_time_window)
        ) \
        .withColumn(
            'prev_week_orders',
            F.lag('weekly_orders', 1).over(category_time_window)
        ) \
        .withColumn(
            'revenue_wow_pct',
            F.when(
                F.col('prev_week_revenue').isNotNull() &
                (F.col('prev_week_revenue') > 0),
                F.round(
                    (F.col('weekly_revenue') - F.col('prev_week_revenue')) /
                    F.col('prev_week_revenue') * 100,
                    2
                )
            ).otherwise(None)
        ) \
        .withColumn(
            'trend',
            F.when(F.col('revenue_wow_pct') > 10, 'GROWING_FAST')
             .when(F.col('revenue_wow_pct') > 0, 'GROWING')
             .when(F.col('revenue_wow_pct').isNull(), 'BASELINE')
             .when(F.col('revenue_wow_pct') > -10, 'DECLINING')
             .otherwise('DECLINING_FAST')
        ) \
        .withColumn(
            'revenue_rank_this_week',
            F.dense_rank().over(
                Window.partitionBy('order_week')
                .orderBy(F.desc('weekly_revenue'))
            )
        ) \
        .orderBy('order_week', 'revenue_rank_this_week')

    logger.info(f"Category trends: {with_trends.count()} rows")
    return with_trends


# ════════════════════════════════════════════
# MAIN JOB RUNNER
# ════════════════════════════════════════════

def save_results(df: DataFrame, name: str, output_path: str):
    """Write DataFrame to Parquet. Create output dir if needed."""
    import os
    path = f"{output_path}/{name}"
    os.makedirs(path, exist_ok=True)
    df.coalesce(1).write.mode('overwrite').parquet(path)
    logger.info(f"Saved {name} → {path}")


def main():
    logger.info("=== MEESHO DAILY BATCH JOB STARTED ===")
    start = datetime.now()

    spark = create_spark_session("MeeshoDailyBatch")
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # EXTRACT
        logger.info("Step 1/6: Extracting data...")
        orders_df = generate_meesho_data(spark)
        orders_df.cache()

        # VALIDATE INPUT
        logger.info("Step 2/6: Validating input...")
        input_valid = run_all_checks(
            orders_df,
            table_name="orders_raw",
            required_columns=["order_id", "seller_id", "amount_inr"],
            key_columns=["order_id"]
        )
        if not input_valid:
            raise ValueError("Input validation failed")

        output_path = "data/processed"

        # TRANSFORM AND LOAD
        logger.info("Step 3/6: Daily revenue...")
        daily_revenue = compute_daily_revenue(orders_df)
        save_results(daily_revenue, "daily_revenue", output_path)

        logger.info("Step 4/6: Cohort retention...")
        cohort = compute_cohort_retention(orders_df)
        save_results(cohort, "cohort_retention", output_path)

        logger.info("Step 5/6: Seller performance...")
        seller_perf = compute_seller_performance(orders_df)
        save_results(seller_perf, "seller_performance", output_path)

        logger.info("Step 6/6: Fraud patterns + Category trends...")
        fraud = detect_batch_fraud_patterns(orders_df)
        save_results(fraud, "fraud_patterns", output_path)

        trends = compute_category_trends(orders_df)
        save_results(trends, "category_trends", output_path)

        orders_df.unpersist()

        # PRINT SAMPLE RESULTS
        print("\n" + "="*60)
        print("BATCH JOB RESULTS")
        print("="*60)

        print("\nDAILY REVENUE (last 3 days):")
        daily_revenue.orderBy(
            F.desc('order_date'), F.desc('total_revenue')
        ).show(6, truncate=False)

        print("\nSELLER LEADERBOARD (top 5):")
        seller_perf.select(
            'rank', 'seller_id', 'total_revenue',
            'total_orders', 'seller_tier', 'performance_score'
        ).show(5, truncate=False)

        print("\nCOHORT RETENTION:")
        cohort.filter(
            F.col('weeks_since_signup') <= 3
        ).orderBy(
            'cohort_week', 'weeks_since_signup'
        ).show(12, truncate=False)

        print("\nCATEGORY TRENDS (latest week):")
        trends.orderBy(
            F.desc('order_week'), 'revenue_rank_this_week'
        ).show(6, truncate=False)

        print("\nFRAUD RISK (HIGH risk sellers):")
        fraud.filter(
            F.col('fraud_risk_level') == 'HIGH'
        ).select(
            'seller_id', 'fraud_risk_level',
            'round_number_rate', 'off_hours_rate',
            'fraud_signal_count'
        ).show(5, truncate=False)

        elapsed = (datetime.now() - start).total_seconds()
        logger.info(f"\n=== JOB COMPLETE in {elapsed:.1f}s ===")

    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()