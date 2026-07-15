"""
RAG Chatbot — Natural Language Interface to Your Data

RAG = Retrieval Augmented Generation

Without RAG:
LLM knows nothing about your Meesho data.
"Which sellers have delivery problems?" → no answer.

With RAG:
Step 1: Embed user question into vector
Step 2: Search Qdrant for similar review vectors
Step 3: Retrieve actual review text and metadata
Step 4: Pass retrieved context + question to LLM
Step 5: LLM generates answer using your real data

This is the architecture behind every AI assistant
at every tech company using their own data.
ChatGPT with plugins, Notion AI, GitHub Copilot Chat
all use this exact pattern.
"""

import os
import json
import logging
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google import genai
from google.genai import types
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


class MeeshoRAGChatbot:
    """
    Answers business questions using your actual review data.

    Two types of questions it handles:

    Type 1 — Semantic search questions:
    "Find reviews about delivery problems"
    "Show me complaints about Electronics sellers"
    → Uses Qdrant vector search

    Type 2 — Analytics questions:
    "How many negative reviews this week?"
    "Which category has worst sentiment?"
    → Uses DuckDB SQL queries

    Type 3 — Combined questions:
    "Why are customers unhappy with sellers in Jaipur?"
    → Combines both: SQL for stats + Qdrant for examples
    """

    SYSTEM_PROMPT = """You are a business intelligence assistant for Meesho,
an Indian e-commerce platform. You answer questions about customer reviews,
seller performance, and product quality using retrieved data.

Always:
- Base your answers only on the provided context
- Mention specific sellers, categories, or cities when relevant
- Give actionable insights not just raw numbers
- If context is insufficient, say so clearly
- Keep answers concise but complete

Retrieved context from reviews database:
{context}

User question: {question}

Answer based strictly on the context above:"""

    def __init__(self, qdrant_store, duckdb_store):
        api_key = os.getenv('GEMINI_API_KEY')
        self.client = genai.Client(api_key=api_key)
        self.qdrant = qdrant_store
        self.duckdb = duckdb_store

        logger.info("Loading embedding model for query encoding...")
        self.embedder = SentenceTransformer('all-MiniLM-L6-v2')
        logger.info("RAG chatbot ready")

        self.conversation_history = []

    def retrieve_context(self, question: str, n_results: int = 5) -> str:
        """
        Retrieve relevant context for the question.

        Step 1: Embed the question
        Step 2: Search Qdrant for similar reviews
        Step 3: Also run SQL for aggregate stats
        Step 4: Combine both into context string

        Why combine vector search AND SQL:
        Vector search finds relevant individual reviews
        SQL gives aggregate statistics
        Together they give both examples AND numbers
        """
        query_embedding = self.embedder.encode(
            question,
            normalize_embeddings=True
        ).tolist()

        similar_reviews = self.qdrant.semantic_search(
            query_embedding=query_embedding,
            limit=n_results
        )

        sentiment_stats = self.duckdb.get_sentiment_summary()
        urgent_reviews = self.duckdb.get_urgent_reviews(limit=3)

        context_parts = []

        if similar_reviews:
            context_parts.append("RELEVANT REVIEWS FROM DATABASE:")
            for i, review in enumerate(similar_reviews, 1):
                context_parts.append(
                    f"{i}. [{review['sentiment']}] "
                    f"Similarity: {review['similarity_score']:.2f} | "
                    f"Seller: {review.get('seller_id', 'Unknown')} | "
                    f"Summary: {review['summary']} | "
                    f"Text: {review['original_text'][:200]}..."
                )

        if sentiment_stats:
            context_parts.append("\nSENTIMENT DISTRIBUTION:")
            for stat in sentiment_stats:
                context_parts.append(
                    f"- {stat['sentiment']}: {stat['count']} reviews "
                    f"(avg score: {stat['avg_score']}, "
                    f"avg rating: {stat['avg_rating']})"
                )

        if urgent_reviews:
            context_parts.append("\nURGENT ISSUES REQUIRING ACTION:")
            for review in urgent_reviews:
                context_parts.append(
                    f"- [{review['urgency_level']}] "
                    f"Seller {review['seller_id']} in {review['city']}: "
                    f"{review['one_line_summary']}"
                )

        return "\n".join(context_parts) if context_parts else "No relevant data found in database."

    def ask(self, question: str) -> dict:
        """
        Answer a business question using RAG.
        Returns answer with sources.
        """
        logger.info(f"Question: {question}")

        context = self.retrieve_context(question)

        prompt = self.SYSTEM_PROMPT.format(
            context=context,
            question=question
        )

        try:
            response = self.client.models.generate_content(
                model='gemini-1.5-flash',
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0.3
                )
            )

            answer = response.text.strip()

            self.conversation_history.append({
                'question': question,
                'answer': answer,
                'context_used': context[:200] + '...'
            })

            logger.info(f"Answer generated ({len(answer)} chars)")

            return {
                'question': question,
                'answer': answer,
                'context': context,
                'sources_used': len(context.split('\n'))
            }

        except Exception as e:
            logger.error(f"RAG generation failed: {e}")
            return {
                'question': question,
                'answer': f"Sorry, I could not generate an answer: {e}",
                'context': context,
                'sources_used': 0
            }


if __name__ == "__main__":
    from ai_layer.storage import DuckDBStore, QdrantStore

    duck = DuckDBStore()
    qdrant = QdrantStore()
    chatbot = MeeshoRAGChatbot(qdrant, duck)

    test_questions = [
        "What are customers complaining about most?",
        "Which sellers have the most delivery problems?",
        "Show me urgent issues that need immediate attention",
        "What is the overall sentiment of reviews?"
    ]

    print("=== MEESHO AI CHATBOT ===\n")
    for question in test_questions:
        print(f"Q: {question}")
        result = chatbot.ask(question)
        print(f"A: {result['answer']}")
        print("-" * 60)