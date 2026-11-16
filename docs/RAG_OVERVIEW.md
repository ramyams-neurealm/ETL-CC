# RAG in ETL-CC

## What it is and where it is used

ETL-CC uses **retrieval-augmented generation (RAG)** to provide approved,
relevant examples and guidance to the PySpark conversion agent. For each
mapping in a conversion or migration, `RAGRetrievalAgent` retrieves knowledge
before `ConversionAgent` generates code, tests, and configuration. The retrieved
items are passed as `approved_knowledge` in the conversion evidence. RAG
supports conversion; it does not itself generate artifacts, and it is not the
knowledge-ingestion process.

## Retrieval approach

The implementation is **semantic vector retrieval with deterministic metadata
reranking**. It uses PostgreSQL with pgvector cosine distance to find up to 30
nearest eligible knowledge records. It then scores each candidate using a
weighted combination of vector similarity (default weight 0.70) and metadata
match (default weight 0.30). Metadata scoring considers transformation-type
overlap, target framework, and source-vendor match. A record must meet both
minimum thresholds (defaults: vector similarity 0.50 and final relevance 0.65)
to be accepted. Accepted items are ranked by final score, with a stable ID
tie-break, and at most 8 are returned. These settings are configurable.

This is not a keyword/BM25 search: lexical search is not part of this retrieval
path. The internal agent label `PGVECTOR_HYBRID` refers to vector retrieval plus
metadata scoring.

## Knowledge lifecycle and eligibility

Knowledge is stored in `knowledge_base_etl`, including its content, metadata,
approval state, embedding, model, and content hashes. Entries are populated
separately, approved, and embedded by running `python -m etl_cc.rag_backfill`.
The default embedding deployment is `text-embedding-3-small` with 1,536
dimensions. An embedding is generated from the knowledge type, title, selected
metadata, and content. Changed content must be re-embedded.

A record can be retrieved only when it is approved, embedding is complete and
present, its embedding model matches the configured deployment, its embedding
content hash matches the current content hash, and it is global or belongs to
the current repository. Thus a row in the table is not automatically
retrievable. RAG does not add rows to the knowledge table.

## What happens when there is no match

If no candidate passes both thresholds—or there are no eligible records—the
result is `NO_MATCH`; the conversion still receives the RAG result, but there
are no retrieved examples to contribute. RAG is supporting evidence, not a
guarantee that a conversion will pass its output contract, critique, or tests.

## Operations and diagnostics

`rag_retrieval_event_etl` records retrieval-run telemetry, not one row per
knowledge item. It includes candidate/accepted/retrieved counts, similarity,
duration, estimated context tokens, and whether knowledge contributed. The
agent audit/API diagnostics also expose eligible counts, candidate scores,
thresholds, rejection reasons, and embedding input tokens. Context-token count
is an estimate based on characters divided by four.

After deployment, inspect `mappings[].rag` or `mappings[].stages.rag` in
`GET /api/v1/migrations/{migration_id}/dashboard`; conversion workbench and
mapping-conversion responses also include `rag`. A healthy retrieval attempt
can have zero accepted items: check `eligible_knowledge_count`, candidate
rejection reasons, and thresholds to distinguish missing/stale knowledge from
low relevance. `RAG_RETRIEVAL_AGENT` input-token usage is for the embedding
request, not a chat-completion request.
