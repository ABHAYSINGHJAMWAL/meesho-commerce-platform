"""
Meesho AI Intelligence Dashboard

Streamlit app that combines:
1. Review enrichment pipeline — process new reviews
2. Analytics — sentiment breakdown, urgent issues
3. RAG chatbot — natural language questions
4. Seller intelligence — per-seller sentiment

Run with: streamlit run ai_layer/dashboard.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="Meesho AI Intelligence",
    page_icon="🛒",
    layout="wide"
)

from ai_layer.review_generator import ReviewGenerator
from ai_layer.ai_enrichment import AIEnrichmentPipeline
from ai_layer.storage import DuckDBStore, QdrantStore
from ai_layer.rag_chatbot import MeeshoRAGChatbot


@st.cache_resource
def load_pipeline():
    return AIEnrichmentPipeline()

@st.cache_resource
def load_stores():
    return DuckDBStore(), QdrantStore()

@st.cache_resource
def load_chatbot():
    duck, qdrant = load_stores()
    return MeeshoRAGChatbot(qdrant, duck)


st.title("🛒 Meesho AI Commerce Intelligence")
st.caption("Real-time review analysis powered by Gemini AI + RAG")

tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Analytics Dashboard",
    "🤖 AI Chatbot",
    "⚡ Process Reviews",
    "🏪 Seller Intelligence"
])


with tab1:
    st.header("Review Analytics")
    duck, qdrant = load_stores()

    col1, col2, col3 = st.columns(3)

    summary = duck.get_sentiment_summary()
    if summary:
        total = sum(s['count'] for s in summary)
        positive = next((s['count'] for s in summary if s['sentiment'] == 'POSITIVE'), 0)
        negative = next((s['count'] for s in summary if s['sentiment'] == 'NEGATIVE'), 0)

        with col1:
            st.metric("Total Reviews", total)
        with col2:
            st.metric(
                "Positive Reviews",
                positive,
                delta=f"{positive/max(1,total)*100:.1f}%"
            )
        with col3:
            st.metric(
                "Negative Reviews",
                negative,
                delta=f"-{negative/max(1,total)*100:.1f}%",
                delta_color="inverse"
            )

        st.subheader("Sentiment Distribution")
        df = pd.DataFrame(summary)
        if not df.empty:
            st.bar_chart(df.set_index('sentiment')['count'])

    st.subheader("🚨 Urgent Issues")
    urgent = duck.get_urgent_reviews(limit=5)
    if urgent:
        df_urgent = pd.DataFrame(urgent)
        st.dataframe(df_urgent, use_container_width=True)
    else:
        st.info("No urgent issues found. Process some reviews first.")


with tab2:
    st.header("🤖 Ask About Your Data")
    st.caption("Ask any business question about customer reviews in natural language")

    chatbot = load_chatbot()

    example_questions = [
        "What are customers complaining about most?",
        "Which sellers have delivery problems?",
        "What issues need urgent attention?",
        "How is overall customer sentiment?"
    ]

    st.write("**Example questions:**")
    cols = st.columns(2)
    for i, q in enumerate(example_questions):
        with cols[i % 2]:
            if st.button(q, key=f"example_{i}"):
                st.session_state['current_question'] = q

    question = st.text_input(
        "Your question:",
        value=st.session_state.get('current_question', ''),
        placeholder="e.g. Which sellers have the most negative reviews?"
    )

    if st.button("Ask", type="primary") and question:
        with st.spinner("Searching reviews and generating answer..."):
            result = chatbot.ask(question)

        st.subheader("Answer")
        st.write(result['answer'])

        with st.expander("View retrieved context"):
            st.text(result['context'])


with tab3:
    st.header("⚡ Process New Reviews")
    st.caption("Generate and enrich customer reviews with Gemini AI")

    n_reviews = st.slider("Number of reviews to process", 2, 20, 5)

    if st.button("Generate and Enrich Reviews", type="primary"):
        gen = ReviewGenerator()
        reviews = [r.to_dict() for r in gen.generate_batch(n_reviews)]

        pipeline = load_pipeline()
        duck, qdrant = load_stores()

        with st.spinner(f"Enriching {n_reviews} reviews with Gemini AI..."):
            progress = st.progress(0)

            successful = []
            failed = []

            for i, review in enumerate(reviews):
                enriched = pipeline.enrich_review(review)
                if enriched.enrichment_success:
                    successful.append(enriched)
                    duck.insert_enriched_review(enriched)
                    qdrant.insert_review(enriched)
                else:
                    failed.append(enriched)
                progress.progress((i + 1) / len(reviews))

        st.success(f"✅ Enriched {len(successful)} reviews successfully")
        if failed:
            st.warning(f"⚠️ {len(failed)} reviews sent to DLQ")

        if successful:
            st.subheader("Sample Enriched Reviews")
            data = []
            for r in successful[:5]:
                data.append({
                    'Review': r.original_text[:80] + '...',
                    'Sentiment': r.sentiment,
                    'Score': r.sentiment_score,
                    'Category': r.primary_category,
                    'Urgency': r.urgency_level,
                    'Summary': r.one_line_summary,
                    'Action Needed': r.requires_action
                })
            st.dataframe(pd.DataFrame(data), use_container_width=True)


with tab4:
    st.header("🏪 Seller Intelligence")
    st.caption("AI-powered sentiment analysis per seller")

    duck, _ = load_stores()

    seller_id = st.text_input(
        "Enter Seller ID",
        placeholder="e.g. SELL-0001"
    )

    if st.button("Analyse Seller") and seller_id:
        with st.spinner(f"Analysing {seller_id}..."):
            data = duck.get_seller_sentiment(seller_id)

        if data:
            d = data[0]
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Reviews", d['total_reviews'])
            with col2:
                st.metric("Avg Sentiment", f"{d['avg_sentiment']:.2f}")
            with col3:
                st.metric("Positive", d['positive_count'])
            with col4:
                st.metric("Negative", d['negative_count'])

            if d['avg_sentiment'] >= 0.7:
                st.success("This seller has strong positive sentiment")
            elif d['avg_sentiment'] >= 0.4:
                st.warning("This seller has mixed sentiment — monitor closely")
            else:
                st.error("This seller has poor sentiment — action required")
        else:
            st.info(f"No reviews found for {seller_id}. Process some reviews first.")