"""
AI Enrichment Pipeline — Core
Uses google-genai (new package) + sentence-transformers
"""

import os
import time
import json
import logging
import sys
from typing import Optional
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google import genai
from google.genai import types
from sentence_transformers import SentenceTransformer
from pydantic import BaseModel, field_validator
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ReviewEnrichment(BaseModel):
    sentiment: str
    sentiment_score: float
    primary_category: str
    issue_type: Optional[str] = None
    urgency_level: str
    key_entities: list
    one_line_summary: str
    requires_action: bool
    suggested_action: Optional[str] = None

    @field_validator('sentiment')
    @classmethod
    def validate_sentiment(cls, v):
        allowed = {'POSITIVE', 'NEGATIVE', 'NEUTRAL'}
        v = v.upper()
        if v not in allowed:
            raise ValueError(f"Invalid sentiment: {v}")
        return v

    @field_validator('sentiment_score')
    @classmethod
    def validate_score(cls, v):
        v = float(v)
        if not 0.0 <= v <= 1.0:
            raise ValueError(f"Score out of range: {v}")
        return round(v, 3)

    @field_validator('urgency_level')
    @classmethod
    def validate_urgency(cls, v):
        allowed = {'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'}
        v = v.upper()
        if v not in allowed:
            raise ValueError(f"Invalid urgency: {v}")
        return v

    @field_validator('primary_category')
    @classmethod
    def validate_category(cls, v):
        allowed = {
            'PRODUCT_QUALITY', 'DELIVERY', 'PRICING',
            'FRAUD_SUSPICION', 'CUSTOMER_SERVICE',
            'RETURN_REFUND', 'GENERAL_FEEDBACK'
        }
        v = v.upper()
        if v not in allowed:
            raise ValueError(f"Invalid category: {v}")
        return v


@dataclass
class EnrichedReview:
    review_id: str
    original_text: str
    seller_id: str
    product_category: str
    city: str
    rating: int
    timestamp: str
    sentiment: str
    sentiment_score: float
    primary_category: str
    issue_type: Optional[str]
    urgency_level: str
    key_entities: list
    one_line_summary: str
    requires_action: bool
    suggested_action: Optional[str]
    embedding: list
    enriched_at: str
    model_used: str
    enrichment_success: bool
    validation_passed: bool

    def to_dict(self):
        return asdict(self)


class AIEnrichmentPipeline:

    PROMPT = """You are a data extraction system for an Indian e-commerce platform.
Analyze this customer review and return ONLY a valid JSON object.

Review: "{review_text}"
Category: {product_category}
Rating: {rating}/5

Return ONLY this JSON with no extra text:
{{
    "sentiment": "POSITIVE" or "NEGATIVE" or "NEUTRAL",
    "sentiment_score": float 0.0 to 1.0,
    "primary_category": one of PRODUCT_QUALITY or DELIVERY or PRICING or FRAUD_SUSPICION or CUSTOMER_SERVICE or RETURN_REFUND or GENERAL_FEEDBACK,
    "issue_type": "string describing issue" or null,
    "urgency_level": "LOW" or "MEDIUM" or "HIGH" or "CRITICAL",
    "key_entities": ["list", "of", "entities"],
    "one_line_summary": "one sentence under 20 words",
    "requires_action": true or false,
    "suggested_action": "action string" or null
}}"""

    def __init__(self):
        api_key = os.getenv('GEMINI_API_KEY')
        if not api_key:
            raise ValueError("GEMINI_API_KEY not found in .env — get free key at aistudio.google.com")

        self.client = genai.Client(api_key=api_key)
        logger.info("Gemini client initialized")

        logger.info("Loading embedding model (downloads ~90MB first time)...")
        self.embedder = SentenceTransformer('all-MiniLM-L6-v2')
        logger.info("Embedding model ready")

        self.stats = {
            'processed': 0, 'llm_success': 0, 'llm_failed': 0,
            'validation_passed': 0, 'validation_failed': 0, 'dlq_count': 0
        }

    def call_llm(self, prompt: str, max_retries: int = 3) -> Optional[dict]:
        for attempt in range(max_retries):
            try:
                response = self.client.models.generate_content(
                    model='gemini-1.5-flash',
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        temperature=0.1,
                        response_mime_type='application/json'
                    )
                )
                raw = response.text.strip()
                if raw.startswith('```'):
                    raw = raw.split('```')[1]
                    if raw.startswith('json'):
                        raw = raw[4:]
                parsed = json.loads(raw)
                self.stats['llm_success'] += 1
                return parsed
            except json.JSONDecodeError as e:
                logger.warning(f"Invalid JSON from LLM attempt {attempt+1}: {e}")
            except Exception as e:
                wait = 2 ** attempt
                logger.warning(f"LLM failed attempt {attempt+1}: {e} — waiting {wait}s")
                if attempt < max_retries - 1:
                    time.sleep(wait)

        self.stats['llm_failed'] += 1
        return None

    def generate_embedding(self, text: str) -> list:
        return self.embedder.encode(
            text,
            convert_to_tensor=False,
            normalize_embeddings=True
        ).tolist()

    def validate(self, raw: dict) -> Optional[ReviewEnrichment]:
        try:
            enrichment = ReviewEnrichment(**raw)
            self.stats['validation_passed'] += 1
            return enrichment
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            self.stats['validation_failed'] += 1
            return None

    def enrich_review(self, review: dict) -> EnrichedReview:
        self.stats['processed'] += 1
        time.sleep(0.1)  # rate limiting for free tier

        text = review.get('review_text', '')
        category = review.get('product_category', 'Unknown')
        rating = review.get('rating', 3)

        prompt = self.PROMPT.format(
            review_text=text,
            product_category=category,
            rating=rating
        )

        raw_output = self.call_llm(prompt)
        enrichment = None
        if raw_output:
            enrichment = self.validate(raw_output)

        embedding = self.generate_embedding(text)

        if enrichment:
            logger.info(
                f"Enriched {review.get('review_id','')} — "
                f"sentiment={enrichment.sentiment} ({enrichment.sentiment_score:.2f}) "
                f"urgency={enrichment.urgency_level}"
            )
            return EnrichedReview(
                review_id=review.get('review_id', ''),
                original_text=text,
                seller_id=review.get('seller_id', ''),
                product_category=category,
                city=review.get('city', ''),
                rating=rating,
                timestamp=review.get('timestamp', ''),
                sentiment=enrichment.sentiment,
                sentiment_score=enrichment.sentiment_score,
                primary_category=enrichment.primary_category,
                issue_type=enrichment.issue_type,
                urgency_level=enrichment.urgency_level,
                key_entities=enrichment.key_entities,
                one_line_summary=enrichment.one_line_summary,
                requires_action=enrichment.requires_action,
                suggested_action=enrichment.suggested_action,
                embedding=embedding,
                enriched_at=datetime.now(timezone.utc).isoformat(),
                model_used='gemini-1.5-flash',
                enrichment_success=True,
                validation_passed=True
            )
        else:
            self.stats['dlq_count'] += 1
            fallback = 'POSITIVE' if rating >= 4 else 'NEGATIVE' if rating <= 2 else 'NEUTRAL'
            logger.warning(f"Fallback for {review.get('review_id','')} — sent to DLQ")
            return EnrichedReview(
                review_id=review.get('review_id', ''),
                original_text=text,
                seller_id=review.get('seller_id', ''),
                product_category=category,
                city=review.get('city', ''),
                rating=rating,
                timestamp=review.get('timestamp', ''),
                sentiment=fallback,
                sentiment_score=rating / 5.0,
                primary_category='GENERAL_FEEDBACK',
                issue_type=None,
                urgency_level='LOW',
                key_entities=[],
                one_line_summary='LLM enrichment failed — fallback applied',
                requires_action=False,
                suggested_action=None,
                embedding=embedding,
                enriched_at=datetime.now(timezone.utc).isoformat(),
                model_used='fallback',
                enrichment_success=False,
                validation_passed=False
            )

    def process_batch(self, reviews: list) -> tuple:
        successful, failed = [], []
        logger.info(f"Processing {len(reviews)} reviews...")

        for i, review in enumerate(reviews):
            enriched = self.enrich_review(review)
            if enriched.enrichment_success:
                successful.append(enriched)
            else:
                failed.append(enriched)
            if (i + 1) % 5 == 0:
                logger.info(f"Progress: {i+1}/{len(reviews)} | Success: {len(successful)} | DLQ: {len(failed)}")

        logger.info(
            f"\n=== ENRICHMENT COMPLETE ===\n"
            f"Processed: {self.stats['processed']}\n"
            f"LLM success: {self.stats['llm_success']}\n"
            f"LLM failed: {self.stats['llm_failed']}\n"
            f"Validation passed: {self.stats['validation_passed']}\n"
            f"DLQ count: {self.stats['dlq_count']}"
        )
        return successful, failed


if __name__ == "__main__":
    from ai_layer.review_generator import ReviewGenerator

    gen = ReviewGenerator()
    reviews = [r.to_dict() for r in gen.generate_batch(5)]

    pipeline = AIEnrichmentPipeline()
    successful, failed = pipeline.process_batch(reviews)

    print(f"\nSuccessfully enriched: {len(successful)}")
    print(f"Failed (DLQ): {len(failed)}")

    if successful:
        s = successful[0]
        print(f"\nSample enriched review:")
        print(f"  Text:      {s.original_text[:80]}...")
        print(f"  Sentiment: {s.sentiment} ({s.sentiment_score:.2f})")
        print(f"  Category:  {s.primary_category}")
        print(f"  Urgency:   {s.urgency_level}")
        print(f"  Summary:   {s.one_line_summary}")
        print(f"  Entities:  {s.key_entities}")
        print(f"  Embedding: {len(s.embedding)} dimensions")
        print(f"  Action:    {s.requires_action} — {s.suggested_action}")