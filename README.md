# CrawlFlow

A production-ready, single-file FastAPI web scraping pipeline orchestrator with real-time monitoring.

## Features

- **Pipeline Management**: Create, configure, and delete reusable scraping pipelines
- **CSS Selector Extraction**: Define multiple selectors per pipeline to extract text, href, or src attributes
- **Background Job Execution**: Async job processing with full state machine (PENDING → RUNNING → COMPLETED/FAILED)
- **Real-Time Dashboard**: Dark-themed responsive UI with live status updates via 3-second polling
- **Execution History**: Track the 30 most recent job runs with expandable data inspection
- **Error Handling**: Comprehensive handling for network timeouts, HTTP errors, and parsing failures
- **Single-File Architecture**: Entire application contained in `main.py` for easy deployment

## Dashboard Preview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  CrawlFlow - Web Scraping Pipeline Orchestrator                             │
├─────────────────────────────────┬───────────────────────────────────────────┤
│  CONTROL DESK                   │  ORCHESTRATION MATRIX                     │
│  ┌───────────────────────────┐  │  ┌─────────────────────────────────────┐  │
│  │ Pipeline Name             │  │  │ Active Pipelines                    │  │
│  │ [____________________]    │  │  │ ┌─────────────────────────────────┐ │  │
│  │                           │  │  │ │ News Scraper    [COMPLETED]     │ │  │
│  │ Target URL                │  │  │ │ https://example.com             │ │  │
│  │ [____________________]    │  │  │ │ 3 selectors    [Delete] [Run]   │ │  │
│  │                           │  │  │ └─────────────────────────────────┘ │  │
│  │ Selectors                 │  │  │                                     │  │
│  │ ┌─────┬────────┬──────┐   │  │  │ Execution History                   │  │
│  │ │ Key │CSS Sel │ Attr │   │  │  │ ┌─────────┬────────┬───────────┐   │  │
│  │ ├─────┼────────┼──────┤   │  │  │ │Pipeline │ Status │ Completed │   │  │
│  │ │title│h1      │ text │   │  │  │ ├─────────┼────────┼───────────┤   │  │
│  │ │links│a.nav   │ href │   │  │  │ │News     │COMPLETED│Jun 17 2pm│   │  │
│  │ └─────┴────────┴──────┘   │  │  │ │News     │FAILED  │Jun 17 1pm│   │  │
│  │ [+ Add Selector]          │  │  │ └─────────┴────────┴───────────┘   │  │
│  │                           │  │  └─────────────────────────────────────┘  │
│  │ [    Create Pipeline    ] │  │                                           │
│  └───────────────────────────┘  │                                           │
└─────────────────────────────────┴───────────────────────────────────────────┘
```

## Requirements

- Python 3.10+
- pip or uv package manager

### Dependencies

| Package | Purpose |
|---------|---------|
| fastapi | Web framework with async support |
| uvicorn | ASGI server |
| sqlalchemy | Async ORM with SQLite |
| aiosqlite | Async SQLite driver |
| httpx | Async HTTP client |
| beautifulsoup4 | HTML parsing |
| pydantic | Data validation |

## Installation

### Using pip

```bash
# Clone the repository
git clone <repository-url>
cd crawlflow

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install fastapi uvicorn sqlalchemy aiosqlite httpx beautifulsoup4 pydantic
```

### Using uv

```bash
# Clone the repository
git clone <repository-url>
cd crawlflow

# Create virtual environment and install dependencies
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv pip install fastapi uvicorn sqlalchemy aiosqlite httpx beautifulsoup4 pydantic
```

## Running the Application

Start the server with uvicorn:

```bash
uvicorn main:app --reload
```

The application will be available at:
- **Dashboard**: http://localhost:8000/
- **API Documentation**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/health

For production deployment:

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
```

## API Reference

### Health Check

```bash
curl http://localhost:8000/health
```

Response:
```json
{"status": "ok"}
```

### Create Pipeline

```bash
curl -X POST http://localhost:8000/api/pipelines \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Example Scraper",
    "target_url": "https://example.com",
    "selectors": [
      {"key": "title", "selector": "h1", "attribute": "text"},
      {"key": "links", "selector": "a", "attribute": "href"}
    ]
  }'
```

Response (201 Created):
```json
{
  "id": 1,
  "name": "Example Scraper",
  "target_url": "https://example.com",
  "selectors": [
    {"key": "title", "selector": "h1", "attribute": "text"},
    {"key": "links", "selector": "a", "attribute": "href"}
  ],
  "created_at": "2024-01-15T10:30:00"
}
```

### List Pipelines

```bash
curl http://localhost:8000/api/pipelines
```

Response:
```json
[
  {
    "id": 1,
    "name": "Example Scraper",
    "target_url": "https://example.com",
    "selectors": [...],
    "created_at": "2024-01-15T10:30:00",
    "latest_status": "COMPLETED"
  }
]
```

### Trigger Pipeline Execution

```bash
curl -X POST http://localhost:8000/api/pipelines/1/trigger
```

Response (202 Accepted):
```json
{
  "job_id": 1,
  "pipeline_id": 1,
  "status": "PENDING",
  "message": "Job scheduled successfully"
}
```

### Get Execution History

```bash
curl http://localhost:8000/api/history
```

Response:
```json
[
  {
    "id": 1,
    "pipeline_id": 1,
    "pipeline_name": "Example Scraper",
    "target_url": "https://example.com",
    "status": "COMPLETED",
    "started_at": "2024-01-15T10:31:00",
    "completed_at": "2024-01-15T10:31:05",
    "extracted_data": {
      "data": {
        "title": ["Example Domain"],
        "links": ["https://www.iana.org/domains/example"]
      }
    },
    "error_message": null
  }
]
```

### Delete Pipeline

```bash
curl -X DELETE http://localhost:8000/api/pipelines/1
```

Response: 204 No Content

## Job Status States

| Status | Description |
|--------|-------------|
| `PENDING` | Job created, waiting for worker to pick up |
| `RUNNING` | Worker is actively scraping the target URL |
| `COMPLETED` | Scraping finished successfully with extracted data |
| `FAILED` | Scraping failed due to network error, HTTP error, or parsing failure |

## Selector Configuration

Each selector extracts data from matched HTML elements:

| Attribute | Description |
|-----------|-------------|
| `text` | Extract the text content of the element |
| `href` | Extract the href attribute (for links) |
| `src` | Extract the src attribute (for images, scripts) |

Example selectors:
```json
[
  {"key": "headlines", "selector": "h1, h2, h3", "attribute": "text"},
  {"key": "nav_links", "selector": "nav a", "attribute": "href"},
  {"key": "images", "selector": "img.thumbnail", "attribute": "src"}
]
```

## Architecture

CrawlFlow uses a single-file architecture where all components are contained in `main.py`:

- **Database Models**: SQLAlchemy ORM models for Pipeline and JobRun
- **Pydantic Schemas**: Request/response validation with comprehensive error messages
- **Scraper Engine**: Async HTTP client with timeout handling and BeautifulSoup parsing
- **Background Worker**: State machine implementation for job lifecycle management
- **API Endpoints**: FastAPI routes with dependency injection
- **Dashboard UI**: Embedded HTML/CSS/JavaScript single-page application

The SQLite database (`crawlflow.db`) is created automatically on first run.

## License

MIT
