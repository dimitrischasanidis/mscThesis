# RAG Infrastructure Design

> Generic, data-agnostic RAG creator for **any document corpus**.
> First-class languages: **Greek (el) · English (en) · German (de)**.
> Fully on-premise. No cloud. GPU: **NVIDIA RTX PRO 4000 Blackwell (24 GB GDDR7)**.

---

## 1. Goals & Constraints

| Dimension | Decision |
|-----------|----------|
| **Privacy** | Fully on-premise — no data leaves the server |
| **Corpus** | Any — filesystem, SQL DB, JSONL, APIs; swapped via source adapters |
| **Languages** | Greek (el), English (en), German (de) — per-document, per-chunk |
| **GPU** | RTX PRO 4000 Blackwell, 24 GB GDDR7 — inference + fine-tuning |
| **Scope** | Retrieval-augmented generation + optional LLM fine-tuning |
| **Vector dim** | 1024 (`multilingual-e5-large` / `BGE-M3`) — consistent across stores |

---

## 2. Architecture

```
Any document corpus
(filesystem / SQL / JSONL / API)
          │
          ▼
   rag/sources/<adapter>.py
   iter_documents() → Document
          │
          ▼
   rag/ingestion_to_index/
   chunk.py  →  embed.py
          │
          ├──► pgvector  (chunks table, HNSW)      ← canonical + metadata joins
          ├──► Qdrant    (multilingual collection)  ← production retrieval
          └──► FAISS     (local .index files)       ← offline experiments
                    │
                    ▼
   rag/retrieval/retriever.py
   BM25 sparse (per-lang) + dense embed
   → bge-reranker-v2-m3 cross-encoder
   → MMR dedup → top-k
          │
          ▼
   rag/serving/api.py (FastAPI)
   lang detect → model router
   → vLLM (OpenAI-compatible, port 8000)
          │
          ▼
   Any frontend (Streamlit, REST client, …)

   Traces → Langfuse (self-hosted)
   Logs   → Loki / Grafana (existing)
```

---

## 3. Source-Adapter Abstraction

**All corpus specifics live here.** The rest of the RAG stack consumes
`Document` objects exclusively — swap the adapter, everything else stays.

```python
# rag/sources/base.py
from dataclasses import dataclass, field
from typing import Iterator, Protocol

@dataclass
class Document:
    doc_id:   str               # globally unique within the corpus
    text:     str               # full document text (pre-extracted)
    lang:     str | None = None # "el" | "en" | "de" | None → auto-detect
    metadata: dict = field(default_factory=dict)  # arbitrary, for filtering

class DocumentSource(Protocol):
    """Implement this to plug in any corpus."""
    def iter_documents(self) -> Iterator[Document]: ...
```

### Example adapters to implement

| Adapter | File | Description |
|---------|------|-------------|
| **Filesystem** | `rag/sources/filesystem.py` | Walk a directory; txt/md files read directly; pdf files extracted via `pdfminer.six` + OCR fallback |
| **SQL** | `rag/sources/sql.py` | Generic `DSN + SELECT` → `Document`; caller maps columns in config |
| **JSONL** | `rag/sources/jsonl.py` | `{doc_id, text, lang?, metadata?}` per line |

Example filesystem adapter:

```python
# rag/sources/filesystem.py
import os
from pathlib import Path
from .base import Document, DocumentSource

class FilesystemSource(DocumentSource):
    def __init__(self, root: str, default_lang: str | None = None):
        self.root = Path(root)
        self.default_lang = default_lang

    def iter_documents(self) -> Iterator[Document]:
        for path in self.root.rglob("*"):
            if path.suffix in (".txt", ".md"):
                yield Document(
                    doc_id=str(path.relative_to(self.root)),
                    text=path.read_text(encoding="utf-8"),
                    lang=self.default_lang,
                    metadata={"source": str(path), "filename": path.name},
                )
```

The SQL adapter accepts any DSN + a SELECT that returns at least `doc_id` and
`text`; `lang` and `metadata` columns are optional. This makes it trivial to
point at an existing database without coupling to its schema.

---

## 4. Generic Storage Schema

The RAG layer owns its **own tables** — independent of the ingestion schema.

```sql
-- rag/schema.sql
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS rag_documents (
    doc_id      TEXT PRIMARY KEY,
    source      TEXT NOT NULL,          -- adapter name / origin tag
    lang        TEXT,                   -- "el" | "en" | "de" | null
    metadata    JSONB DEFAULT '{}',
    indexed_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rag_chunks (
    chunk_id    BIGSERIAL PRIMARY KEY,
    doc_id      TEXT NOT NULL REFERENCES rag_documents(doc_id) ON DELETE CASCADE,
    chunk_index INT  NOT NULL,
    text        TEXT NOT NULL,
    lang        TEXT,                   -- detected lang of this chunk
    embedding   vector(1024),
    UNIQUE (doc_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_rag_chunks_doc    ON rag_chunks (doc_id);
CREATE INDEX IF NOT EXISTS idx_rag_chunks_lang   ON rag_chunks (lang);
-- Activate after index is populated:
-- CREATE INDEX idx_rag_chunks_hnsw ON rag_chunks
--     USING hnsw (embedding vector_cosine_ops) WITH (m=16, ef_construction=64);
```

Qdrant and FAISS carry the same `chunk_id` + `metadata` as their primary key /
payload — `chunk_id` is the join key back to `rag_chunks`.

---

## 5. Chunking — Language-Aware (el / en / de)

**File:** `rag/ingestion_to_index/chunk.py`

### Language detection

```python
from langdetect import detect   # pip install langdetect
# or: fasttext-langdetect (faster, offline model)
SUPPORTED = {"el", "en", "de"}

def detect_lang(text: str, hint: str | None) -> str | None:
    if hint in SUPPORTED:
        return hint
    try:
        lang = detect(text)
        return lang if lang in SUPPORTED else None
    except Exception:
        return None
```

### Sentence splitting (per language)

```python
import spacy
# Download: python -m spacy download el_core_news_sm en_core_web_sm de_core_news_sm

_MODELS = {
    "el": "el_core_news_sm",
    "en": "en_core_web_sm",
    "de": "de_core_news_sm",
}
_NLP: dict = {}

def get_nlp(lang: str):
    if lang not in _NLP:
        _NLP[lang] = spacy.load(_MODELS[lang], disable=["ner", "parser"])
        _NLP[lang].add_pipe("sentencizer")
    return _NLP[lang]
```

### Chunking strategy

| Chunk type | Logic | Target tokens |
|------------|-------|---------------|
| Short texts (≤512 tok) | Kept whole | — |
| Long texts | Sliding window over sentences | 512 tok / 64 tok overlap |

Metadata passed through from `Document.metadata` verbatim — the chunker does
not inspect or require specific keys.

---

## 6. Embedding Models

| Model | Dim | el | en | de | Notes |
|-------|-----|----|----|----|-------|
| **`intfloat/multilingual-e5-large`** ✅ | 1024 | ✅ | ✅ | ✅ | **Default** — balanced quality across all 3 |
| `BAAI/bge-m3` | 1024 | ✅ | ✅ | ✅ | Dense + sparse + ColBERT in one; enables native hybrid; upgrade path |
| `Alibaba-NLP/gte-multilingual-base` | 768 | ✅ | ✅ | ✅ | Faster/smaller; needs schema change to `vector(768)` |

All three support el/en/de out of the box. The 1024-dim constraint keeps
pgvector, Qdrant, and FAISS index definitions stable across model swaps between
e5-large and BGE-M3.

**Embedding fine-tune path:** use `sentence-transformers` contrastive training on
domain-specific positive/negative pairs from your corpus (any language). The
RTX PRO 4000 handles batch 128–256 for bi-encoders at 7B scale.

---

## 7. Vector Stores

All three are maintained in parallel. `chunk_id` is the canonical ID across all
stores; `doc_id` + `lang` + arbitrary `metadata` ride as payload.

### 7.1 pgvector (primary)

Best for: joins, metadata-rich filtering, transactions.

```sql
-- Generic metadata filter + similarity search
SELECT c.chunk_id, c.doc_id, c.lang, c.text,
       1 - (c.embedding <=> $1::vector) AS score
FROM rag_chunks c
JOIN rag_documents d USING (doc_id)
WHERE d.lang = 'de'                              -- any metadata field
  AND d.metadata->>'category' = 'legal'
ORDER BY score DESC
LIMIT 20;
```

### 7.2 Qdrant (production retrieval)

```yaml
# docker/compose.rag.yml (excerpt)
qdrant:
  image: qdrant/qdrant:v1.9.7
  container_name: rag-qdrant
  ports:
    - "6333:6333"
    - "6334:6334"
  volumes:
    - ./qdrant_data:/qdrant/storage
  networks:
    - backend
```

Collection setup — named vectors so both dense and sparse live in one
collection (for BGE-M3 hybrid):

```python
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, SparseVectorParams

client = QdrantClient("localhost", port=6333)
client.create_collection(
    collection_name="rag",
    vectors_config={
        "dense": VectorParams(size=1024, distance=Distance.COSINE),
    },
    sparse_vectors_config={
        "sparse": SparseVectorParams(),  # for BGE-M3 sparse leg
    },
)
```

Payload filtering on any `metadata` key (lang, category, date, source, …) is
native — no JOIN needed.

### 7.3 FAISS (offline experiments)

```python
import faiss, numpy as np

res = faiss.StandardGpuResources()
index = faiss.index_cpu_to_gpu(res, 0, faiss.IndexFlatIP(1024))

# Add L2-normalised chunk embeddings
faiss.normalize_L2(vectors)   # vectors: float32 np.ndarray (N, 1024)
index.add(vectors)

# Search
faiss.normalize_L2(query)
scores, indices = index.search(query, k=20)
# map indices → chunk_id via parallel array
```

### Vector Store Comparison

| Dimension | pgvector | Qdrant | FAISS |
|-----------|----------|--------|-------|
| **Use case** | Joins + metadata | Production RAG | Offline ablations |
| **Hybrid search** | No (or pg_bm25 ext) | Native (dense+sparse) | No |
| **GPU** | No | Quantization | Full GPU |
| **Metadata filter** | SQL WHERE + JSONB | Payload filter | External |
| **Scale** | Medium | High | Single-process |
| **Setup cost** | Schema only | Docker service | `pip install` |

---

## 8. Retrieval Pipeline

**File:** `rag/retrieval/retriever.py`

```
Query text
    │
    ├── Detect query lang (el / en / de / unknown)
    │
    ├── Dense embed (e5-large or BGE-M3)
    │       └──► Qdrant dense search (top-50)
    │
    ├── Sparse / BM25
    │       ├── per-lang stemmer (PyStemmer greek/english/german)
    │       └──► Qdrant sparse search (top-50)  OR  BM25S in-process
    │
    └── Hybrid fusion (Reciprocal Rank Fusion)
            │
            ▼
       Cross-encoder reranker: bge-reranker-v2-m3  (GPU, multilingual el/en/de)
       top-50 → top-10
            │
            ▼
       MMR deduplication (λ=0.5) → final top-k (default k=5)
            │
            ▼
       Retrieved chunks + metadata → generation context
```

**Per-language BM25 stemming:**

```python
import Stemmer  # PyStemmer
_STEMMERS = {
    "el": Stemmer.Stemmer("greek"),
    "en": Stemmer.Stemmer("english"),
    "de": Stemmer.Stemmer("german"),
}

def tokenize(text: str, lang: str) -> list[str]:
    stemmer = _STEMMERS.get(lang, _STEMMERS["en"])
    tokens = text.lower().split()
    return stemmer.stemWords(tokens)
```

---

## 9. Generation — Per-Language Model Routing

**Engine:** [vLLM](https://github.com/vllm-project/vllm) — OpenAI-compatible
REST API, continuous batching, PagedAttention, Blackwell CUDA 12.8+.

### Router Logic

```python
# rag/serving/router.py
LANG_TO_MODEL = {
    "el": "ilsp/Llama-Krikri-8B-Instruct",   # Greek-specialized
    "en": "Qwen/Qwen2.5-7B-Instruct",         # strong English
    "de": "google/gemma-2-9b-it",             # strong German
}
FALLBACK_MODEL = "Qwen/Qwen2.5-7B-Instruct"  # multilingual fallback

def select_model(query_lang: str | None) -> str:
    return LANG_TO_MODEL.get(query_lang or "", FALLBACK_MODEL)
```

vLLM can serve **multiple models simultaneously** if VRAM allows (one 8B bf16
uses ~16 GB — leaves ~8 GB). For >2 models: use vLLM's `--enable-lora` or
swap models per-request (adds cold-start latency ~30s). Document which mode
suits your workload in `configs/rag_config.yml`.

### Model Matrix (24 GB GDDR7)

| Model | Params | VRAM (bf16) | VRAM (4-bit AWQ) | el ★ | en ★ | de ★ | Router target |
|-------|--------|-------------|------------------|------|------|------|---------------|
| **Llama-Krikri-8B-Instruct** | 8B | ~16 GB | ~6 GB | ★★★★★ | ★★★★☆ | ★★☆☆☆ | `el` |
| **Meltemi-7B-Instruct-v1.5** | 7B | ~14 GB | ~5 GB | ★★★★★ | ★★★☆☆ | ★★☆☆☆ | `el` (alt) |
| **Gemma-2-9B-it** | 9B | ~18 GB | ~7 GB | ★★★★☆ | ★★★★☆ | ★★★★☆ | `de`, `en` |
| **Gemma-3-12B-it** | 12B | ~24 GB | ~8 GB | ★★★★☆ | ★★★★★ | ★★★★★ | `de`, `en` (4-bit only) |
| **Qwen2.5-7B-Instruct** | 7B | ~14 GB | ~5 GB | ★★★☆☆ | ★★★★★ | ★★★★☆ | `en`, fallback |
| **Qwen2.5-14B-Instruct** | 14B | OOM bf16 | ~10 GB | ★★★☆☆ | ★★★★★ | ★★★★★ | 4-bit only |
| **Llama-3.1-8B-Instruct** | 8B | ~16 GB | ~6 GB | ★★★☆☆ | ★★★★★ | ★★★☆☆ | `en` (alt) |

**Recommended defaults:**
- `el`: `Llama-Krikri-8B-Instruct` (16 GB bf16 — leaves reranker headroom)
- `de` + `en`: `Gemma-2-9B-it` (18 GB bf16) or `Qwen2.5-7B` (14 GB, co-loads with Krikri in 24 GB split)

**For co-loading two models in 24 GB:** Krikri-8B-4bit (~6 GB) + Gemma-2-9B-4bit (~7 GB) + reranker (~1 GB) = ~14 GB. Leaves buffer for KV cache.

---

## 10. Fine-Tuning Pipeline

> Adapt any multilingual LLM to a new domain corpus without changing the RAG
> architecture.

**File:** `rag/finetune/`

### Dataset Construction

```python
# rag/finetune/build_dataset.py
# Generic: read from rag_chunks + rag_documents
# Build instruction pairs in the target language

# Type 1 — answer-from-context (core RAG supervision)
{
    "instruction": {
        "el": "Βάσει του εγγράφου, απάντησε στην ερώτηση.",
        "en": "Based on the provided context, answer the question.",
        "de": "Beantworte die Frage anhand des gegebenen Kontexts.",
    }[doc_lang],
    "context": "<retrieved_chunks>",
    "input":   "<user_question>",
    "output":  "<reference_answer>",   # from corpus or synthesized
}

# Type 2 — summarization (synthesized via judge LLM)
{
    "instruction": "Summarize the following document.",
    "input": "<chunk_text>",
    "output": "<llm_generated_summary>",
}
```

Language is detected per document; instruction is chosen to match. Reference
answers can be **human-annotated** or **synthesized** (off-GPU, using a judge
model like Qwen2.5-7B) from your corpus.

### Training Setup (24 GB Blackwell)

**Recommended framework:** [Unsloth](https://github.com/unslothai/unsloth) —
2× faster QLoRA, 60% less VRAM, automatic Blackwell kernel selection.

**Universal QLoRA config (7–8B, any language):**

```yaml
# rag/finetune/configs/default_qlora.yml  (Axolotl format; Unsloth also accepts)
base_model: <your-model-id>    # e.g. ilsp/Llama-Krikri-8B-Instruct
load_in_4bit: true
bnb_4bit_quant_type: nf4
bnb_4bit_compute_dtype: bfloat16

lora_r: 64
lora_alpha: 128
lora_dropout: 0.05
lora_target_modules: [q_proj, k_proj, v_proj, o_proj, gate_proj, up_proj, down_proj]

sequence_len: 4096
micro_batch_size: 2
gradient_accumulation_steps: 8    # effective batch = 16
num_epochs: 3
learning_rate: 2.0e-4
lr_scheduler: cosine
warmup_ratio: 0.05
gradient_checkpointing: true

output_dir: ./checkpoints/<run-name>
```

**VRAM budget per model size:**

| Model size | QLoRA VRAM | Seq 4096 | Fits 24 GB? |
|------------|-----------|----------|-------------|
| 7–8B | ~14–16 GB | ~18–20 GB | ✅ comfortable |
| 9B | ~17 GB | ~21 GB | ✅ with `batch=2` |
| 12–14B | ~20–24 GB | OOM | ✅ seq_len=2048 + batch=1 |

**Merge & serve:**

```bash
python -m peft merge_lora \
  --base_model <your-model-id> \
  --peft_model ./checkpoints/<run-name> \
  --output_dir ./models/<merged-name>
# Load merged dir in vLLM as usual
```

**Experiment tracking:** self-host **MLflow** (port 5000, add to
`docker/compose.rag.yml`) — log loss curves, eval metrics, VRAM peak, model
registry per run. Alternative: TensorBoard (emitted natively by Axolotl/TRL).

---

## 11. Orchestration Framework

**Recommendation: [LlamaIndex](https://github.com/run-llama/llama_index)**

| Framework | Verdict | Reason |
|-----------|---------|--------|
| **LlamaIndex** | ✅ Use | Native `PGVectorStore`, `QdrantVectorStore`; structured metadata filtering; composable query pipelines; good multilingual usage |
| LangChain | Fallback | Larger ecosystem but over-abstracted; harder to debug retrieval; dependency complexity |
| Haystack | Consider later | Production pipeline orientation; steeper learning curve |

Adapter pattern: `DocumentSource.iter_documents()` maps directly to
LlamaIndex `Document` objects. Swap vector store with one config line.

---

## 12. Serving & Integration

### `docker/compose.rag.yml`

```yaml
services:
  qdrant:
    image: qdrant/qdrant:v1.9.7
    container_name: rag-qdrant
    ports:
      - "6333:6333"
      - "6334:6334"
    volumes:
      - ./qdrant_data:/qdrant/storage
    networks:
      - backend

  vllm:
    image: vllm/vllm-openai:latest
    container_name: rag-vllm
    runtime: nvidia
    environment:
      NVIDIA_VISIBLE_DEVICES: all
    command: >
      --model ${VLLM_MODEL:-Qwen/Qwen2.5-7B-Instruct}
      --dtype bfloat16
      --max-model-len 8192
      --gpu-memory-utilization 0.85
    ports:
      - "8000:8000"
    volumes:
      - ~/.cache/huggingface:/root/.cache/huggingface
    networks:
      - backend

  rag-api:
    build:
      context: ./rag/serving
      dockerfile: Dockerfile
    container_name: rag-api
    environment:
      PG_DSN: "host=postgres port=5432 dbname=rag user=rag password=rag"
      QDRANT_URL: "http://qdrant:6333"
      VLLM_URL: "http://vllm:8000"
      LANGFUSE_HOST: "http://langfuse:3000"
    ports:
      - "8080:8080"
    networks:
      - backend
    depends_on:
      - qdrant
      - vllm

  langfuse:
    image: langfuse/langfuse:latest
    container_name: rag-langfuse
    environment:
      DATABASE_URL: "postgresql://rag:rag@postgres:5432/langfuse"
      NEXTAUTH_SECRET: "change-me"
      SALT: "change-me"
    ports:
      - "3002:3000"
    networks:
      - backend

  mlflow:
    image: ghcr.io/mlflow/mlflow:latest
    container_name: rag-mlflow
    command: mlflow server --host 0.0.0.0 --port 5000 --backend-store-uri /mlruns
    volumes:
      - ./mlruns:/mlruns
    ports:
      - "5000:5000"
    networks:
      - backend

networks:
  backend:
    external: true
    name: <project>_backend    # match your project's existing backend network
```

### FastAPI RAG Service

```python
# rag/serving/api.py
from fastapi import FastAPI
from openai import AsyncOpenAI
from .router import select_model
from ..retrieval.retriever import retrieve
from langdetect import detect

app = FastAPI()
vllm = AsyncOpenAI(base_url="http://vllm:8000/v1", api_key="none")

@app.post("/query")
async def query(request: QueryRequest):
    lang = request.lang or detect(request.question)
    chunks = await retrieve(request.question, lang=lang, k=5)
    context = "\n\n---\n\n".join(c.text for c in chunks)
    model = select_model(lang)
    response = await vllm.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPTS[lang]},
            {"role": "user",   "content": f"Context:\n{context}\n\nQuestion: {request.question}"},
        ],
        max_tokens=1024,
        temperature=0.1,
    )
    return {
        "answer":   response.choices[0].message.content,
        "model":    model,
        "lang":     lang,
        "sources":  [{"chunk_id": c.chunk_id, "doc_id": c.doc_id,
                      "score": c.score, "text": c.text[:300]} for c in chunks],
    }
```

System prompts live in `rag/serving/prompts.py`, one per language (el/en/de),
guiding the LLM to answer strictly from context and cite sources.

**Frontend:** any HTTP client, Streamlit chat interface, Gradio, or direct REST
call. The API is corpus-agnostic — no knowledge of the upstream data schema.

---

## 13. Evaluation

**File:** `rag/eval/`

### Retrieval Metrics (per language)

```python
# rag/eval/retrieval_eval.py
# Gold set: list of {query, relevant_chunk_ids, lang}
# Computed per-language + aggregate

metrics = {
    "recall@5":  hits_in_top5  / total,
    "recall@10": hits_in_top10 / total,
    "mrr":       mean(1 / rank_of_first_hit),
    "ndcg@5":    ...,
}
```

Build gold set: annotate 50–100 queries per language with known relevant
documents. Or synthesize via a judge LLM (Qwen2.5-7B off-GPU, one-time).

### RAG Quality Metrics (Ragas)

```bash
pip install ragas
```

```python
from ragas import evaluate
from ragas.metrics import faithfulness, answer_relevancy, context_recall

# Per-language evaluation; judge LLM must be multilingual
result = evaluate(
    dataset,    # HF Dataset: question / answer / contexts / ground_truth / lang
    metrics=[faithfulness, answer_relevancy, context_recall],
    llm=judge_llm,          # Qwen2.5-7B or Gemma-2-9B as evaluator
    embeddings=embed_model,
)
```

**Track per (model, lang, retriever) combination:**

| Model | Lang | Faithfulness | Answer Rel. | Context Recall |
|-------|------|-------------|-------------|----------------|
| Krikri-8B | el | — | — | — |
| Gemma-2-9B | de | — | — | — |
| Qwen2.5-7B | en | — | — | — |

Fill in during Phase 4 experiments.

---

## 14. Directory Layout

```
rag/
├── README.md                          ← this file
│
├── sources/
│   ├── base.py                        # Document dataclass + DocumentSource Protocol
│   ├── filesystem.py                  # txt / md / pdf adapter
│   ├── sql.py                         # generic DSN + SELECT adapter
│   └── jsonl.py                       # JSONL adapter
│
├── ingestion_to_index/
│   ├── chunk.py                       # detect lang → split → sliding window
│   ├── embed.py                       # embed chunks → pgvector + Qdrant + FAISS
│   ├── sync_qdrant.py                 # incremental pgvector → Qdrant sync
│   └── requirements.txt
│
├── retrieval/
│   ├── retriever.py                   # hybrid BM25+dense → rerank → MMR
│   ├── bm25_index.py                  # BM25S index per language
│   └── requirements.txt
│
├── serving/
│   ├── api.py                         # FastAPI /query endpoint
│   ├── router.py                      # lang → model selection
│   ├── prompts.py                     # system prompts in el / en / de
│   ├── Dockerfile
│   └── requirements.txt
│
├── finetune/
│   ├── build_dataset.py               # corpus → multilingual instruction pairs
│   ├── embed_finetune.py              # sentence-transformers contrastive training
│   ├── configs/
│   │   ├── default_qlora.yml          # generic QLoRA (Axolotl/Unsloth)
│   │   └── large_14b_qlora.yml        # 14B variant (4-bit, seq=2048)
│   └── requirements.txt
│
├── eval/
│   ├── retrieval_eval.py              # recall@k, MRR, NDCG per lang
│   ├── ragas_eval.py                  # faithfulness, answer relevancy
│   ├── gold_set.jsonl                 # annotated queries {query, chunks, lang}
│   └── requirements.txt
│
└── configs/
    └── rag_config.yml                 # shared runtime config (see §15)
```

---

## 15. Stack Summary & Runtime Config

### Stack

| Layer | Technology | Notes |
|-------|-----------|-------|
| **Source adapters** | Filesystem / SQL / JSONL | Any corpus via `DocumentSource` |
| **Lang detection** | `langdetect` / `fasttext-langdetect` | Per-doc + per-chunk |
| **Sentence split** | spaCy `el_core_news_sm` / `en_core_web_sm` / `de_core_news_sm` | |
| **Embedding** | `multilingual-e5-large` (default) / `BGE-M3` | 1024-dim, el/en/de |
| **Vector store A** | pgvector HNSW | Joins + metadata |
| **Vector store B** | Qdrant v1.9+ | Production hybrid retrieval |
| **Vector store C** | FAISS GPU | Offline ablations |
| **Sparse / BM25** | BM25S + PyStemmer (el/en/de) | Per-lang stemming |
| **Reranker** | `bge-reranker-v2-m3` | GPU, multilingual cross-encoder |
| **Generation** | vLLM + per-lang router | Krikri (el) / Gemma / Qwen (de, en) |
| **Fine-tuning** | Unsloth / Axolotl QLoRA | 7–8B comfortable; 14B 4-bit |
| **Orchestration** | LlamaIndex | Composable RAG pipelines |
| **API** | FastAPI | `/query` with lang auto-detect |
| **Tracing** | Langfuse (self-hosted) | LLM call traces + latency |
| **Eval** | Ragas + custom | Per-language faithfulness + recall@k |
| **Experiment tracking** | MLflow (self-hosted) | Fine-tune run comparison |
| **Serving infra** | Docker Compose `compose.rag.yml` | Extends existing stack |

### `rag/configs/rag_config.yml`

```yaml
# rag/configs/rag_config.yml
source:
  adapter: filesystem                 # filesystem | sql | jsonl
  params:
    root: /data/corpus                # for filesystem adapter
    # dsn: postgresql://...           # for sql adapter
    # query: "SELECT id, text, lang FROM docs"

languages: [el, en, de]              # expected corpus languages

embedding:
  model: intfloat/multilingual-e5-large
  dim: 1024
  batch_size: 64
  device: cuda

retrieval:
  top_k_candidate: 50
  top_k_final: 5
  reranker: BAAI/bge-reranker-v2-m3
  mmr_lambda: 0.5

generation:
  routing:
    el: ilsp/Llama-Krikri-8B-Instruct
    en: Qwen/Qwen2.5-7B-Instruct
    de: google/gemma-2-9b-it
  fallback: Qwen/Qwen2.5-7B-Instruct
  vllm_url: http://vllm:8000
  max_tokens: 1024
  temperature: 0.1

vector_stores:
  pgvector:
    dsn: ${PG_DSN}
    schema: public
  qdrant:
    url: http://qdrant:6333
    collection: rag
  faiss:
    index_path: ./faiss/rag.index
    id_map_path: ./faiss/rag_ids.npy

observability:
  langfuse_host: http://langfuse:3000
  mlflow_tracking_uri: http://mlflow:5000
```

### Open Questions

1. **VRAM confirmation:** run `nvidia-smi` — confirm 24 GB available before
   selecting bf16 vs 4-bit default.
2. **Multi-model co-load:** if serving Krikri (el) + Gemma (de/en)
   simultaneously, use 4-bit quantization for both (~6+7 = 13 GB) + reranker
   (~1 GB) = ~14 GB total. Feasible on 24 GB with headroom.
3. **BGE-M3 sparse vectors in pgvector:** pgvector does not natively store
   sparse vectors; use Qdrant for hybrid if upgrading from e5-large to BGE-M3.
4. **Langfuse DB:** needs a separate Postgres DB (`langfuse`). Either add a
   `CREATE DATABASE langfuse` to init scripts or use a second postgres service.
5. **German tokenization in BM25:** `PyStemmer("german")` uses
   Snowball-German — sufficient for most DE text; replace with `HanTa` for
   morphologically complex German compounds if precision drops.
6. **spaCy model download in Docker:** add
   `RUN python -m spacy download el_core_news_sm en_core_web_sm de_core_news_sm`
   to the `rag/ingestion_to_index/` Dockerfile.
