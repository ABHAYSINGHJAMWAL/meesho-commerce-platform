"""
Dual Storage Layer

Enriched reviews stored in two places:
1. DuckDB — structured metadata, fast SQL queries
2. Qdrant — vector embeddings, semantic search

Why two storage systems:
DuckDB answers: "How many NEGATIVE reviews in Jaipur this week?"
Qdrant answers: "Find reviews similar to this complaint"

Neither can answer both questions well.
DuckDB cannot do semantic similarity search.
Qdrant cannot do GROUP BY aggregations efficiently.
Using both gives us the best of both worlds.

This is the standard production pattern for
AI-native data applications at companies like
Swiggy, PhonePe, and any company building
AI features on top of existing data.
"""

import os
import json
import logging
from typing import List, Optional
from datetime import datetime, timezone

import duckdb
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance, VectorParams, PointStruct,
    Filter, FieldCondition, MatchValue
)

logger = logging.getLogger(__name__)

COLLECTION_NAME = "meesho_reviews"
EMBEDDING_DIM = 384  # all-MiniLM-L6-v2 output dimension


class DuckDBStore:
    """
    Stores structured review metadata in DuckDB.

    Why DuckDB and not PostgreSQL or BigQuery:
    DuckDB is file-based — no server needed.
    Incredibly fast for analytical queries.
    Perfect for local development and portfolio projects.
    Same SQL you write in BigQuery works in DuckDB.
    In production: swap DuckDB for BigQuery with
    zero changes to your query logic.
    """

    def __init__(self, db_path: str = "data/reviews.duckdb"):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = duckdb.connect(db_path)
        self._create_tables()
        logger.info(f"DuckDB initialized at {db_path}")

    def _create_tables(self):
        """Create tables if they do not exist"""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS enriched_reviews (
                review_id VARCHAR PRIMARY KEY,
                original_text TEXT,
                seller_id VARCHAR,
                product_category VARCHAR,
                city VARCHAR,
                rating INTEGER,
                timestamp TIMESTAMP,
                sentiment VARCHAR,
                sentiment_score FLOAT,
                primary_category VARCHAR,
                issue_type VARCHAR,
                urgency_level VARCHAR,
                key_entities JSON,
                one_line_summary TEXT,
                requires_action BOOLEAN,
                suggested_action TEXT,
                enriched_at TIMESTAMP,
                model_used VARCHAR,
                enrichment_success BOOLEAN,
                validation_passed BOOLEAN
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS dlq_reviews (
                review_id VARCHAR,
                original_text TEXT,
                failure_reason VARCHAR,
                raw_review JSON,
                failed_at TIMESTAMP
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS enrichment_runs (
                run_id VARCHAR PRIMARY KEY,
                run_date TIMESTAMP,
                total_processed INTEGER,
                successful INTEGER,
                failed INTEGER,
                avg_sentiment_score FLOAT
            )
        """)

    def insert_enriched_review(self, enriched) -> bool:
        """Insert one enriched review"""
        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO enriched_reviews VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
            """, [
                enriched.review_id,
                enriched.original_text,
                enriched.seller_id,
                enriched.product_category,
                enriched.city,
                enriched.rating,
                enriched.timestamp,
                enriched.sentiment,
                enriched.sentiment_score,
                enriched.primary_category,
                enriched.issue_type,
                enriched.urgency_level,
                json.dumps(enriched.key_entities),
                enriched.one_line_summary,
                enriched.requires_action,
                enriched.suggested_action,
                enriched.enriched_at,
                enriched.model_used,
                enriched.enrichment_success,
                enriched.validation_passed
            ])
            return True
        except Exception as e:
            logger.error(f"DuckDB insert failed: {e}")
            return False

    def insert_batch(self, enriched_reviews: list) -> int:
        """Insert batch of enriched reviews"""
        success_count = 0
        for review in enriched_reviews:
            if self.insert_enriched_review(review):
                success_count += 1
        logger.info(f"DuckDB: inserted {success_count}/{len(enriched_reviews)} reviews")
        return success_count

    def get_sentiment_summary(self) -> dict:
        """Business analytics on enriched reviews"""
        result = self.conn.execute("""
            SELECT
                sentiment,
                COUNT(*) as count,
                ROUND(AVG(sentiment_score), 3) as avg_score,
                ROUND(AVG(rating), 2) as avg_rating
            FROM enriched_reviews
            WHERE enrichment_success = true
            GROUP BY sentiment
            ORDER BY count DESC
        """).fetchdf()
        return result.to_dict('records')

    def get_urgent_reviews(self, limit: int = 10) -> list:
        """Get reviews requiring immediate action"""
        result = self.conn.execute(f"""
            SELECT
                review_id, seller_id, city,
                urgency_level, primary_category,
                one_line_summary, suggested_action
            FROM enriched_reviews
            WHERE urgency_level IN ('HIGH', 'CRITICAL')
            AND requires_action = true
            ORDER BY
                CASE urgency_level
                    WHEN 'CRITICAL' THEN 1
                    WHEN 'HIGH' THEN 2
                    ELSE 3
                END,
                timestamp DESC
            LIMIT {limit}
        """).fetchdf()
        return result.to_dict('records')

    def get_seller_sentiment(self, seller_id: str) -> dict:
        """Get sentiment breakdown for specific seller"""
        result = self.conn.execute("""
            SELECT
                seller_id,
                COUNT(*) as total_reviews,
                ROUND(AVG(sentiment_score), 3) as avg_sentiment,
                SUM(CASE WHEN sentiment = 'POSITIVE' THEN 1 ELSE 0 END) as positive_count,
                SUM(CASE WHEN sentiment = 'NEGATIVE' THEN 1 ELSE 0 END) as negative_count,
                ROUND(AVG(rating), 2) as avg_rating
            FROM enriched_reviews
            WHERE seller_id = ?
            GROUP BY seller_id
        """, [seller_id]).fetchdf()
        return result.to_dict('records')


class QdrantStore:
    """
    Stores vector embeddings in Qdrant.

    What vectors enable:
    Traditional search: "find reviews containing 'delivery problem'"
    → only finds exact words

    Semantic search: "find reviews about delivery issues"
    → finds reviews about: "package not arrived", "courier problem",
      "took too long", "wrong address" — even without those exact words

    This is how modern search works at Amazon, Flipkart, Swiggy.
    """

    def __init__(self, host: str = "localhost", port: int = 6333):
        self.client = QdrantClient(host=host, port=port)
        self._create_collection()
        logger.info(f"Qdrant connected at {host}:{port}")

    def _create_collection(self):
        """Create vector collection if it does not exist"""
        collections = [c.name for c in self.client.get_collections().collections]

        if COLLECTION_NAME not in collections:
            self.client.create_collection(
                collection_name=COLLECTION_NAME,
                vectors_config=VectorParams(
                    size=EMBEDDING_DIM,
                    distance=Distance.COSINE
                )
            )
            logger.info(f"Created Qdrant collection: {COLLECTION_NAME}")
        else:
            logger.info(f"Using existing Qdrant collection: {COLLECTION_NAME}")

    def insert_review(self, enriched) -> bool:
        """
        Insert review embedding into Qdrant.

        The point structure:
        - id: unique identifier (hash of review_id)
        - vector: 384-dimension embedding
        - payload: metadata for filtering
          (we filter by seller, category, sentiment
           before doing similarity search)
        """
        try:
            point_id = abs(hash(enriched.review_id)) % (2**63)

            self.client.upsert(
                collection_name=COLLECTION_NAME,
                points=[PointStruct(
                    id=point_id,
                    vector=enriched.embedding,
                    payload={
                        "review_id": enriched.review_id,
                        "seller_id": enriched.seller_id,
                        "sentiment": enriched.sentiment,
                        "urgency_level": enriched.urgency_level,
                        "primary_category": enriched.primary_category,
                        "city": enriched.city,
                        "product_category": enriched.product_category,
                        "one_line_summary": enriched.one_line_summary,
                        "original_text": enriched.original_text[:500],
                        "rating": enriched.rating,
                        "requires_action": enriched.requires_action
                    }
                )]
            )
            return True
        except Exception as e:
            logger.error(f"Qdrant insert failed: {e}")
            return False

    def insert_batch(self, enriched_reviews: list) -> int:
        """Insert batch of embeddings"""
        success_count = 0
        for review in enriched_reviews:
            if self.insert_review(review):
                success_count += 1
        logger.info(
            f"Qdrant: inserted {success_count}/{len(enriched_reviews)} embeddings"
        )
        return success_count

    def semantic_search(
        self,
        query_embedding: list,
        limit: int = 5,
        sentiment_filter: Optional[str] = None
    ) -> list:
        """
        Find semantically similar reviews.

        query_embedding: embed the user's search query
        limit: how many similar reviews to return
        sentiment_filter: optionally filter by sentiment

        Example:
        User asks: "reviews about delivery problems"
        We embed this query → search Qdrant
        Returns reviews about late delivery, damaged packages,
        wrong address — even without those exact words
        """
        search_filter = None
        if sentiment_filter:
            search_filter = Filter(
                must=[FieldCondition(
                    key="sentiment",
                    match=MatchValue(value=sentiment_filter)
                )]
            )

        results = self.client.search(
            collection_name=COLLECTION_NAME,
            query_vector=query_embedding,
            limit=limit,
            query_filter=search_filter,
            with_payload=True
        )

        return [
            {
                "review_id": r.payload.get("review_id"),
                "similarity_score": round(r.score, 3),
                "summary": r.payload.get("one_line_summary"),
                "sentiment": r.payload.get("sentiment"),
                "original_text": r.payload.get("original_text"),
                "seller_id": r.payload.get("seller_id"),
                "urgency": r.payload.get("urgency_level")
            }
            for r in results
        ]


if __name__ == "__main__":
    from ai_layer.review_generator import ReviewGenerator
    from ai_layer.ai_enrichment import AIEnrichmentPipeline

    gen = ReviewGenerator()
    reviews = [r.to_dict() for r in gen.generate_batch(3)]

    pipeline = AIEnrichmentPipeline()
    successful, failed = pipeline.process_batch(reviews)

    if successful:
        duck = DuckDBStore()
        qdrant = QdrantStore()

        duck.insert_batch(successful)
        qdrant.insert_batch(successful)

        print("\n=== DUCKDB SENTIMENT SUMMARY ===")
        summary = duck.get_sentiment_summary()
        for row in summary:
            print(f"  {row['sentiment']}: {row['count']} reviews, avg score {row['avg_score']}")

        print("\n=== SEMANTIC SEARCH TEST ===")
        query = "delivery problem package not arrived"
        query_embedding = pipeline.generate_embedding(query)
        results = qdrant.semantic_search(query_embedding, limit=3)
        print(f"Query: '{query}'")
        for r in results:
            print(f"  [{r['similarity_score']:.3f}] {r['summary']}")