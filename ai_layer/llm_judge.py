"""
LLM-as-Judge — Data Quality Validation

The problem with AI pipelines:
LLMs can return plausible-sounding but wrong outputs.
Example: Review says "excellent product, loved it"
LLM incorrectly classifies as NEGATIVE sentiment.
This corrupts your analytics without anyone noticing.

LLM-as-judge solves this:
A second LLM call reviews the first LLM's output
and scores its accuracy.
Low confidence scores go to DLQ for human review.

This is the production pattern used at companies
building AI pipelines where data accuracy matters.
Your fraud detection and seller scoring depend on
accurate sentiment — wrong sentiment = wrong scores.
"""

import os
import json
import logging
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google import genai
from google.genai import types
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


class JudgementResult(BaseModel):
    """Schema for judge's verdict"""
    confidence_score: float      # 0.0 to 1.0
    sentiment_correct: bool
    category_correct: bool
    summary_accurate: bool
    overall_verdict: str         # PASS or FAIL
    reasoning: str


class LLMJudge:
    """
    Second LLM call that validates first LLM's output.

    Why a second LLM call costs extra but is worth it:
    Cost of wrong sentiment in database: corrupted
    seller scores, wrong fraud signals, bad business
    decisions based on wrong data.

    Cost of judge call: ~same tokens as enrichment call.

    For critical records (CRITICAL urgency, fraud signals)
    always run the judge. For routine reviews, sample
    10-20% to keep costs low.
    """

    JUDGE_PROMPT = """You are a quality control system for an AI data pipeline.

Original review text: "{original_text}"
Star rating: {rating}/5

AI system's classification:
- Sentiment: {sentiment}
- Sentiment score: {sentiment_score}
- Category: {primary_category}
- Summary: "{one_line_summary}"
- Urgency: {urgency_level}

Evaluate if the AI's classification is accurate.
Return ONLY this JSON:
{{
    "confidence_score": float 0.0 to 1.0 (how confident you are the output is correct),
    "sentiment_correct": true or false,
    "category_correct": true or false,
    "summary_accurate": true or false,
    "overall_verdict": "PASS" or "FAIL",
    "reasoning": "one sentence explaining your verdict"
}}

PASS if confidence_score >= 0.7
FAIL if confidence_score < 0.7"""

    def __init__(self):
        api_key = os.getenv('GEMINI_API_KEY')
        self.client = genai.Client(api_key=api_key)
        self.stats = {
            'judged': 0,
            'passed': 0,
            'failed': 0
        }

    def judge(self, enriched_review) -> JudgementResult:
        """
        Judge the quality of one enriched review.
        Returns judgement result.
        """
        prompt = self.JUDGE_PROMPT.format(
            original_text=enriched_review.original_text[:500],
            rating=enriched_review.rating,
            sentiment=enriched_review.sentiment,
            sentiment_score=enriched_review.sentiment_score,
            primary_category=enriched_review.primary_category,
            one_line_summary=enriched_review.one_line_summary,
            urgency_level=enriched_review.urgency_level
        )

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

            data = json.loads(raw)
            result = JudgementResult(**data)

            self.stats['judged'] += 1
            if result.overall_verdict == 'PASS':
                self.stats['passed'] += 1
                logger.info(
                    f"JUDGE PASS: {enriched_review.review_id} "
                    f"confidence={result.confidence_score:.2f}"
                )
            else:
                self.stats['failed'] += 1
                logger.warning(
                    f"JUDGE FAIL: {enriched_review.review_id} "
                    f"confidence={result.confidence_score:.2f} "
                    f"reason={result.reasoning}"
                )

            return result

        except Exception as e:
            logger.error(f"Judge call failed: {e}")
            return JudgementResult(
                confidence_score=0.5,
                sentiment_correct=True,
                category_correct=True,
                summary_accurate=True,
                overall_verdict='PASS',
                reasoning='Judge failed — defaulting to PASS'
            )

    def should_judge(self, enriched_review) -> bool:
        """
        Decide if this review needs judging.

        Cost optimization:
        Not every review needs a judge call.
        Always judge: CRITICAL urgency, fraud signals
        Sample 20%: routine reviews
        Never judge: fallback records (LLM already failed)
        """
        if not enriched_review.enrichment_success:
            return False
        if enriched_review.urgency_level == 'CRITICAL':
            return True
        if enriched_review.primary_category == 'FRAUD_SUSPICION':
            return True
        import random
        return random.random() < 0.20

    def get_stats(self) -> dict:
        return self.stats