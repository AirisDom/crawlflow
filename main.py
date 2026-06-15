import json
import traceback
from contextlib import asynccontextmanager
from datetime import datetime
from collections.abc import AsyncGenerator
from typing import Literal

import httpx
from bs4 import BeautifulSoup
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, HttpUrl, field_validator
from sqlalchemy import ForeignKey, Text, event, select, func
from sqlalchemy.ext.asyncio import AsyncAttrs, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

DATABASE_URL = "sqlite+aiosqlite:///./crawlflow.db"

HTTPX_CONNECT_TIMEOUT = 10.0
HTTPX_READ_TIMEOUT = 30.0
HTTPX_WRITE_TIMEOUT = 10.0
HTTPX_POOL_TIMEOUT = 10.0
HTTPX_TIMEOUT = httpx.Timeout(
    connect=HTTPX_CONNECT_TIMEOUT,
    read=HTTPX_READ_TIMEOUT,
    write=HTTPX_WRITE_TIMEOUT,
    pool=HTTPX_POOL_TIMEOUT,
)

engine = create_async_engine(DATABASE_URL, echo=False)


@event.listens_for(engine.sync_engine, "connect")
def enable_sqlite_fk(dbapi_conn, connection_record):
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


async_session = async_sessionmaker(engine, expire_on_commit=False)


class Base(AsyncAttrs, DeclarativeBase):
    pass


class Pipeline(Base):
    __tablename__ = "pipelines"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    target_url: Mapped[str] = mapped_column(Text, nullable=False)
    selectors_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

    job_runs: Mapped[list["JobRun"]] = relationship(back_populates="pipeline")


class JobRun(Base):
    __tablename__ = "job_runs"

    id: Mapped[int] = mapped_column(primary_key=True)
    pipeline_id: Mapped[int] = mapped_column(ForeignKey("pipelines.id"), nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="PENDING")
    started_at: Mapped[datetime | None] = mapped_column(default=None)
    completed_at: Mapped[datetime | None] = mapped_column(default=None)
    extracted_data: Mapped[str | None] = mapped_column(Text, default=None)
    error_message: Mapped[str | None] = mapped_column(Text, default=None)

    pipeline: Mapped["Pipeline"] = relationship(back_populates="job_runs")


class SelectorConfig(BaseModel):
    key: str
    selector: str
    attribute: Literal["text", "href", "src"]


class PipelineCreate(BaseModel):
    name: str
    target_url: HttpUrl
    selectors: list[SelectorConfig]

    @field_validator("name")
    @classmethod
    def name_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("name cannot be empty")
        return v.strip()

    @field_validator("selectors")
    @classmethod
    def selectors_not_empty(cls, v: list[SelectorConfig]) -> list[SelectorConfig]:
        if not v:
            raise ValueError("at least one selector is required")
        return v

    def selectors_to_json(self) -> str:
        return json.dumps([s.model_dump() for s in self.selectors])


class PipelineResponse(BaseModel):
    id: int
    name: str
    target_url: str
    selectors: list[SelectorConfig]
    created_at: datetime

    model_config = {"from_attributes": True}

    @classmethod
    def from_orm_model(cls, pipeline: Pipeline) -> "PipelineResponse":
        selectors_data = json.loads(pipeline.selectors_json)
        selectors = [SelectorConfig(**s) for s in selectors_data]
        return cls(
            id=pipeline.id,
            name=pipeline.name,
            target_url=pipeline.target_url,
            selectors=selectors,
            created_at=pipeline.created_at,
        )


class PipelineWithStatusResponse(BaseModel):
    id: int
    name: str
    target_url: str
    selectors: list[SelectorConfig]
    created_at: datetime
    latest_status: str | None

    model_config = {"from_attributes": True}

    @classmethod
    def from_orm_with_status(cls, pipeline: Pipeline, latest_status: str | None) -> "PipelineWithStatusResponse":
        selectors_data = json.loads(pipeline.selectors_json)
        selectors = [SelectorConfig(**s) for s in selectors_data]
        return cls(
            id=pipeline.id,
            name=pipeline.name,
            target_url=pipeline.target_url,
            selectors=selectors,
            created_at=pipeline.created_at,
            latest_status=latest_status,
        )


class TriggerResponse(BaseModel):
    job_id: int
    pipeline_id: int
    status: str
    message: str = "Job scheduled successfully"


class JobRunResponse(BaseModel):
    id: int
    pipeline_id: int
    status: str
    started_at: datetime | None
    completed_at: datetime | None
    extracted_data: dict | list | None
    error_message: str | None

    model_config = {"from_attributes": True}

    @classmethod
    def from_orm_model(cls, job_run: JobRun) -> "JobRunResponse":
        extracted = None
        if job_run.extracted_data:
            try:
                extracted = json.loads(job_run.extracted_data)
            except json.JSONDecodeError:
                extracted = None
        return cls(
            id=job_run.id,
            pipeline_id=job_run.pipeline_id,
            status=job_run.status,
            started_at=job_run.started_at,
            completed_at=job_run.completed_at,
            extracted_data=extracted,
            error_message=job_run.error_message,
        )


class HistoryResponse(BaseModel):
    id: int
    pipeline_id: int
    pipeline_name: str
    target_url: str
    status: str
    started_at: datetime | None
    completed_at: datetime | None
    extracted_data: dict | list | None
    error_message: str | None

    model_config = {"from_attributes": True}

    @classmethod
    def from_job_and_pipeline(cls, job_run: JobRun, pipeline: Pipeline) -> "HistoryResponse":
        extracted = None
        if job_run.extracted_data:
            try:
                extracted = json.loads(job_run.extracted_data)
            except json.JSONDecodeError:
                extracted = None
        return cls(
            id=job_run.id,
            pipeline_id=job_run.pipeline_id,
            pipeline_name=pipeline.name,
            target_url=pipeline.target_url,
            status=job_run.status,
            started_at=job_run.started_at,
            completed_at=job_run.completed_at,
            extracted_data=extracted,
            error_message=job_run.error_message,
        )


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield


app = FastAPI(lifespan=lifespan)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with async_session() as session:
        yield session


@app.get("/health")
async def health():
    return {"status": "ok"}


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="description" content="CrawlFlow - Web Scraping Pipeline Orchestrator">
    <title>CrawlFlow Dashboard</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-900 text-gray-100 min-h-screen">
    <div class="container mx-auto px-4 py-8">
        <header class="mb-8">
            <h1 class="text-3xl font-bold text-white">CrawlFlow</h1>
            <p class="text-gray-400 mt-1">Web Scraping Pipeline Orchestrator</p>
        </header>

        <div class="grid grid-cols-1 lg:grid-cols-2 gap-8">
            <section id="control-desk" class="bg-gray-800 rounded-lg p-6 shadow-lg">
                <h2 class="text-xl font-semibold text-white mb-4 border-b border-gray-700 pb-2">Control Desk</h2>
                <form id="pipeline-form" class="space-y-4">
                    <div>
                        <label for="pipeline-name" class="block text-sm font-medium text-gray-300 mb-1">Pipeline Name</label>
                        <input type="text" id="pipeline-name" name="name" required
                            class="w-full px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition duration-200"
                            placeholder="Enter pipeline name">
                    </div>
                    <div>
                        <label for="target-url" class="block text-sm font-medium text-gray-300 mb-1">Target URL</label>
                        <input type="url" id="target-url" name="target_url" required
                            class="w-full px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition duration-200"
                            placeholder="https://example.com">
                    </div>
                    <div>
                        <label class="block text-sm font-medium text-gray-300 mb-2">Selectors</label>
                        <div id="selectors-container" class="space-y-2">
                            <div class="selector-row flex gap-2 items-center">
                                <input type="text" name="selector_key" required
                                    class="flex-1 px-3 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent text-sm"
                                    placeholder="Key name">
                                <input type="text" name="selector_css" required
                                    class="flex-1 px-3 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent text-sm"
                                    placeholder="CSS selector">
                                <select name="selector_attr"
                                    class="px-3 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent text-sm">
                                    <option value="text">Text</option>
                                    <option value="href">href</option>
                                    <option value="src">src</option>
                                </select>
                                <button type="button" onclick="removeSelector(this)" class="px-2 py-2 bg-red-600 hover:bg-red-700 rounded-lg text-white transition duration-200">
                                    <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                                    </svg>
                                </button>
                            </div>
                        </div>
                        <button type="button" onclick="addSelector()"
                            class="mt-2 px-3 py-1.5 bg-gray-600 hover:bg-gray-500 rounded-lg text-sm text-white transition duration-200 flex items-center gap-1">
                            <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" />
                            </svg>
                            Add Selector
                        </button>
                    </div>
                    <div id="form-feedback" class="hidden rounded-lg p-3 text-sm"></div>
                    <button type="submit" id="submit-btn"
                        class="w-full px-4 py-3 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-800 disabled:cursor-not-allowed rounded-lg text-white font-medium transition duration-200 flex items-center justify-center gap-2">
                        <span>Create Pipeline</span>
                        <svg id="submit-spinner" class="hidden animate-spin h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                            <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                            <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                        </svg>
                    </button>
                </form>
            </section>

            <section id="orchestration-matrix" class="bg-gray-800 rounded-lg p-6 shadow-lg">
                <h2 class="text-xl font-semibold text-white mb-4 border-b border-gray-700 pb-2">Orchestration Matrix</h2>
                <div class="space-y-6">
                    <div id="pipeline-grid">
                        <h3 class="text-lg font-medium text-gray-300 mb-3">Active Pipelines</h3>
                        <div id="pipelines-container" class="grid grid-cols-1 gap-3">
                            <p id="no-pipelines-msg" class="text-gray-400">No pipelines configured yet.</p>
                        </div>
                    </div>
                    <div id="history-timeline">
                        <h3 class="text-lg font-medium text-gray-300 mb-3">Execution History</h3>
                        <div id="history-container" class="overflow-x-auto">
                            <table class="w-full text-sm text-left">
                                <thead class="text-xs text-gray-400 uppercase bg-gray-700/50">
                                    <tr>
                                        <th class="px-3 py-2 rounded-tl-lg">Pipeline</th>
                                        <th class="px-3 py-2">Status</th>
                                        <th class="px-3 py-2">Started</th>
                                        <th class="px-3 py-2 rounded-tr-lg">Completed</th>
                                    </tr>
                                </thead>
                                <tbody id="history-tbody" class="divide-y divide-gray-700">
                                    <tr><td colspan="4" class="px-3 py-4 text-gray-400 text-center">No job runs yet.</td></tr>
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </section>
        </div>
    </div>

    <script>
        function addSelector() {
            const container = document.getElementById('selectors-container');
            const row = document.createElement('div');
            row.className = 'selector-row flex gap-2 items-center';
            row.innerHTML = `
                <input type="text" name="selector_key" required
                    class="flex-1 px-3 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent text-sm"
                    placeholder="Key name">
                <input type="text" name="selector_css" required
                    class="flex-1 px-3 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent text-sm"
                    placeholder="CSS selector">
                <select name="selector_attr"
                    class="px-3 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent text-sm">
                    <option value="text">Text</option>
                    <option value="href">href</option>
                    <option value="src">src</option>
                </select>
                <button type="button" onclick="removeSelector(this)" class="px-2 py-2 bg-red-600 hover:bg-red-700 rounded-lg text-white transition duration-200">
                    <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                    </svg>
                </button>
            `;
            container.appendChild(row);
        }

        function removeSelector(button) {
            const container = document.getElementById('selectors-container');
            if (container.children.length > 1) {
                button.closest('.selector-row').remove();
            }
        }

        function showFeedback(message, isError) {
            const feedback = document.getElementById('form-feedback');
            feedback.textContent = message;
            feedback.className = isError
                ? 'rounded-lg p-3 text-sm bg-red-900/50 border border-red-600 text-red-200'
                : 'rounded-lg p-3 text-sm bg-emerald-900/50 border border-emerald-600 text-emerald-200';
            feedback.classList.remove('hidden');
            if (!isError) {
                setTimeout(() => feedback.classList.add('hidden'), 5000);
            }
        }

        function setLoading(loading) {
            const btn = document.getElementById('submit-btn');
            const spinner = document.getElementById('submit-spinner');
            btn.disabled = loading;
            spinner.classList.toggle('hidden', !loading);
        }

        document.getElementById('pipeline-form').addEventListener('submit', async function(e) {
            e.preventDefault();
            setLoading(true);
            document.getElementById('form-feedback').classList.add('hidden');

            const name = document.getElementById('pipeline-name').value.trim();
            const targetUrl = document.getElementById('target-url').value.trim();

            const selectorRows = document.querySelectorAll('.selector-row');
            const selectors = [];
            for (const row of selectorRows) {
                const key = row.querySelector('[name="selector_key"]').value.trim();
                const selector = row.querySelector('[name="selector_css"]').value.trim();
                const attribute = row.querySelector('[name="selector_attr"]').value;
                if (key && selector) {
                    selectors.push({ key, selector, attribute });
                }
            }

            if (selectors.length === 0) {
                showFeedback('At least one selector is required', true);
                setLoading(false);
                return;
            }

            try {
                const response = await fetch('/api/pipelines', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ name, target_url: targetUrl, selectors })
                });

                if (response.ok) {
                    const data = await response.json();
                    showFeedback(`Pipeline "${data.name}" created successfully (ID: ${data.id})`, false);
                    document.getElementById('pipeline-form').reset();
                    const container = document.getElementById('selectors-container');
                    while (container.children.length > 1) {
                        container.lastChild.remove();
                    }
                    container.querySelector('[name="selector_key"]').value = '';
                    container.querySelector('[name="selector_css"]').value = '';
                    container.querySelector('[name="selector_attr"]').value = 'text';
                    fetchPipelines();
                } else {
                    const errorData = await response.json();
                    const errorMsg = errorData.detail || JSON.stringify(errorData);
                    showFeedback(`Error: ${errorMsg}`, true);
                }
            } catch (err) {
                showFeedback(`Network error: ${err.message}`, true);
            } finally {
                setLoading(false);
            }
        });

        function truncateUrl(url, maxLength = 40) {
            if (url.length <= maxLength) return url;
            return url.substring(0, maxLength - 3) + '...';
        }

        function getStatusBadge(status) {
            if (!status) {
                return '';
            }
            switch (status) {
                case 'RUNNING':
                    return '<span class="px-2 py-0.5 text-xs font-medium rounded bg-yellow-500/20 text-yellow-300 border border-yellow-500 animate-pulse">RUNNING</span>';
                case 'COMPLETED':
                    return '<span class="px-2 py-0.5 text-xs font-medium rounded bg-emerald-500/10 text-emerald-400 border-2 border-emerald-500">COMPLETED</span>';
                case 'FAILED':
                    return '<span class="px-2 py-0.5 text-xs font-bold rounded bg-rose-600 text-white">FAILED</span>';
                case 'PENDING':
                    return '<span class="px-2 py-0.5 text-xs font-medium rounded bg-gray-600 text-gray-300 border border-gray-500">PENDING</span>';
                default:
                    return `<span class="px-2 py-0.5 text-xs font-medium rounded bg-gray-600 text-gray-300">${status}</span>`;
            }
        }

        function renderPipelineTile(pipeline) {
            const selectorCount = pipeline.selectors ? pipeline.selectors.length : 0;
            return `
                <div class="bg-gray-700 border border-gray-600 rounded-lg p-4 hover:border-gray-500 transition duration-200">
                    <div class="flex justify-between items-start mb-2">
                        <h4 class="font-medium text-white truncate flex-1 mr-2">${pipeline.name}</h4>
                        ${getStatusBadge(pipeline.latest_status)}
                    </div>
                    <p class="text-sm text-gray-400 mb-2 truncate" title="${pipeline.target_url}">${truncateUrl(pipeline.target_url)}</p>
                    <div class="flex justify-between items-center">
                        <span class="text-xs text-gray-500">${selectorCount} selector${selectorCount !== 1 ? 's' : ''}</span>
                        <button onclick="triggerPipeline(${pipeline.id})"
                            class="px-3 py-1.5 bg-blue-600 hover:bg-blue-700 rounded text-sm text-white font-medium transition duration-200 flex items-center gap-1">
                            <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z" />
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                            </svg>
                            Run
                        </button>
                    </div>
                </div>
            `;
        }

        async function fetchPipelines() {
            try {
                const response = await fetch('/api/pipelines');
                if (response.ok) {
                    const pipelines = await response.json();
                    const container = document.getElementById('pipelines-container');
                    const noMsg = document.getElementById('no-pipelines-msg');

                    if (pipelines.length === 0) {
                        noMsg.classList.remove('hidden');
                        container.innerHTML = '';
                        container.appendChild(noMsg);
                    } else {
                        noMsg.classList.add('hidden');
                        container.innerHTML = pipelines.map(renderPipelineTile).join('');
                    }
                    if (typeof pollingState !== 'undefined') {
                        pollingState.pipelinesErrors = 0;
                    }
                } else {
                    if (typeof pollingState !== 'undefined') {
                        pollingState.pipelinesErrors++;
                    }
                }
            } catch (err) {
                if (typeof pollingState !== 'undefined') {
                    pollingState.pipelinesErrors++;
                }
                console.error('Failed to fetch pipelines:', err);
            }
        }

        async function triggerPipeline(pipelineId) {
            try {
                const response = await fetch(`/api/pipelines/${pipelineId}/trigger`, {
                    method: 'POST'
                });
                if (response.ok) {
                    fetchPipelines();
                } else {
                    const errorData = await response.json();
                    alert(`Failed to trigger pipeline: ${errorData.detail || 'Unknown error'}`);
                }
            } catch (err) {
                alert(`Network error: ${err.message}`);
            }
        }

        function formatDateTime(isoString) {
            if (!isoString) return '-';
            const date = new Date(isoString);
            return date.toLocaleString('en-US', {
                month: 'short', day: 'numeric',
                hour: '2-digit', minute: '2-digit', second: '2-digit'
            });
        }

        function toggleJobData(jobId) {
            const dataRow = document.getElementById(`job-data-${jobId}`);
            const toggleBtn = document.getElementById(`toggle-btn-${jobId}`);
            if (dataRow.classList.contains('hidden')) {
                dataRow.classList.remove('hidden');
                toggleBtn.innerHTML = `
                    <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 15l7-7 7 7" />
                    </svg>`;
            } else {
                dataRow.classList.add('hidden');
                toggleBtn.innerHTML = `
                    <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
                    </svg>`;
            }
        }

        function renderHistoryRow(job) {
            const hasData = job.status === 'COMPLETED' && job.extracted_data;
            const hasError = job.status === 'FAILED' && job.error_message;
            const isExpandable = hasData || hasError;

            let expandButton = '';
            if (isExpandable) {
                expandButton = `
                    <button id="toggle-btn-${job.id}" onclick="toggleJobData(${job.id})"
                        class="ml-2 p-1 text-gray-400 hover:text-white transition duration-200" title="View details">
                        <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
                        </svg>
                    </button>`;
            }

            let dataContent = '';
            if (hasData) {
                dataContent = JSON.stringify(job.extracted_data, null, 2);
            } else if (hasError) {
                dataContent = job.error_message;
            }

            let expandableRow = '';
            if (isExpandable) {
                const bgColor = hasError ? 'bg-rose-900/20 border-rose-800' : 'bg-gray-900/50 border-gray-700';
                expandableRow = `
                    <tr id="job-data-${job.id}" class="hidden">
                        <td colspan="4" class="px-3 py-2">
                            <div class="${bgColor} border rounded-lg p-3 max-h-64 overflow-auto">
                                <pre class="text-xs text-gray-300 whitespace-pre-wrap break-words font-mono">${dataContent}</pre>
                            </div>
                        </td>
                    </tr>`;
            }

            return `
                <tr class="hover:bg-gray-700/30 transition duration-150">
                    <td class="px-3 py-3">
                        <div class="flex items-center">
                            <span class="text-white font-medium truncate max-w-[120px]" title="${job.pipeline_name}">${job.pipeline_name}</span>
                            ${expandButton}
                        </div>
                    </td>
                    <td class="px-3 py-3">${getStatusBadge(job.status)}</td>
                    <td class="px-3 py-3 text-gray-400 text-xs">${formatDateTime(job.started_at)}</td>
                    <td class="px-3 py-3 text-gray-400 text-xs">${formatDateTime(job.completed_at)}</td>
                </tr>
                ${expandableRow}
            `;
        }

        async function fetchHistory() {
            try {
                const response = await fetch('/api/history');
                if (response.ok) {
                    const history = await response.json();
                    const tbody = document.getElementById('history-tbody');

                    if (history.length === 0) {
                        tbody.innerHTML = '<tr><td colspan="4" class="px-3 py-4 text-gray-400 text-center">No job runs yet.</td></tr>';
                    } else {
                        tbody.innerHTML = history.map(renderHistoryRow).join('');
                    }
                    if (typeof pollingState !== 'undefined') {
                        pollingState.historyErrors = 0;
                    }
                } else {
                    if (typeof pollingState !== 'undefined') {
                        pollingState.historyErrors++;
                    }
                }
            } catch (err) {
                if (typeof pollingState !== 'undefined') {
                    pollingState.historyErrors++;
                }
                console.error('Failed to fetch history:', err);
            }
        }

        const pollingState = {
            pipelinesInterval: null,
            historyInterval: null,
            pipelinesErrors: 0,
            historyErrors: 0,
            maxErrors: 10,
            pollInterval: 3000,
            isPolling: false
        };

        function startPolling() {
            if (pollingState.isPolling) return;
            pollingState.isPolling = true;
            pollingState.pipelinesErrors = 0;
            pollingState.historyErrors = 0;

            fetchPipelines();
            fetchHistory();

            pollingState.pipelinesInterval = setInterval(function() {
                if (pollingState.pipelinesErrors >= pollingState.maxErrors) {
                    console.warn('Stopping pipelines polling due to repeated errors');
                    clearInterval(pollingState.pipelinesInterval);
                    pollingState.pipelinesInterval = null;
                    return;
                }
                fetchPipelines();
            }, pollingState.pollInterval);

            pollingState.historyInterval = setInterval(function() {
                if (pollingState.historyErrors >= pollingState.maxErrors) {
                    console.warn('Stopping history polling due to repeated errors');
                    clearInterval(pollingState.historyInterval);
                    pollingState.historyInterval = null;
                    return;
                }
                fetchHistory();
            }, pollingState.pollInterval);
        }

        function stopPolling() {
            pollingState.isPolling = false;
            if (pollingState.pipelinesInterval) {
                clearInterval(pollingState.pipelinesInterval);
                pollingState.pipelinesInterval = null;
            }
            if (pollingState.historyInterval) {
                clearInterval(pollingState.historyInterval);
                pollingState.historyInterval = null;
            }
        }

        document.addEventListener('visibilitychange', function() {
            if (document.hidden) {
                stopPolling();
            } else {
                startPolling();
            }
        });

        window.addEventListener('beforeunload', function() {
            stopPolling();
        });

        startPolling();
    </script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse(content=DASHBOARD_HTML)


@app.post("/api/pipelines", response_model=PipelineResponse, status_code=201)
async def create_pipeline(
    payload: PipelineCreate,
    db: AsyncSession = Depends(get_db),
) -> PipelineResponse:
    pipeline = Pipeline(
        name=payload.name,
        target_url=str(payload.target_url),
        selectors_json=payload.selectors_to_json(),
    )
    db.add(pipeline)
    await db.commit()
    await db.refresh(pipeline)
    return PipelineResponse.from_orm_model(pipeline)


async def scrape_url(url: str, selectors: list[SelectorConfig]) -> dict[str, list[str]]:
    timeout = httpx.Timeout(
        connect=HTTPX_CONNECT_TIMEOUT,
        read=HTTPX_READ_TIMEOUT,
        write=HTTPX_WRITE_TIMEOUT,
        pool=HTTPX_POOL_TIMEOUT,
    )
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.get(url)
        response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    extracted_data: dict[str, list[str]] = {}

    for selector_config in selectors:
        elements = soup.select(selector_config.selector)
        values: list[str] = []
        for element in elements:
            if selector_config.attribute == "text":
                values.append(element.get_text(strip=True))
            elif selector_config.attribute == "href":
                href = element.get("href")
                if href:
                    values.append(str(href))
            elif selector_config.attribute == "src":
                src = element.get("src")
                if src:
                    values.append(str(src))
        extracted_data[selector_config.key] = values

    return extracted_data


async def run_pipeline_job(pipeline_id: int, job_run_id: int) -> None:
    async with async_session() as db:
        result = await db.execute(select(JobRun).where(JobRun.id == job_run_id))
        job_run = result.scalar_one_or_none()
        if job_run is None:
            return

        result = await db.execute(select(Pipeline).where(Pipeline.id == pipeline_id))
        pipeline = result.scalar_one_or_none()
        if pipeline is None:
            job_run.status = "FAILED"
            job_run.completed_at = datetime.utcnow()
            job_run.error_message = f"Pipeline with id {pipeline_id} not found"
            await db.commit()
            return

        job_run.status = "RUNNING"
        job_run.started_at = datetime.utcnow()
        await db.commit()

        try:
            selectors_data = json.loads(pipeline.selectors_json)
            selectors = [SelectorConfig(**s) for s in selectors_data]
            extracted = await scrape_url(pipeline.target_url, selectors)
            job_run.extracted_data = json.dumps(extracted)
            job_run.status = "COMPLETED"
            job_run.completed_at = datetime.utcnow()
        except Exception:
            job_run.status = "FAILED"
            job_run.completed_at = datetime.utcnow()
            job_run.error_message = traceback.format_exc()

        await db.commit()


@app.get("/api/pipelines", response_model=list[PipelineWithStatusResponse])
async def list_pipelines(
    db: AsyncSession = Depends(get_db),
) -> list[PipelineWithStatusResponse]:
    max_id_subquery = (
        select(JobRun.pipeline_id, func.max(JobRun.id).label("max_id"))
        .group_by(JobRun.pipeline_id)
        .subquery()
    )

    latest_status_subquery = (
        select(JobRun.pipeline_id, JobRun.status)
        .join(max_id_subquery, JobRun.id == max_id_subquery.c.max_id)
        .subquery()
    )

    query = (
        select(Pipeline, latest_status_subquery.c.status)
        .outerjoin(latest_status_subquery, Pipeline.id == latest_status_subquery.c.pipeline_id)
        .order_by(Pipeline.id)
    )

    result = await db.execute(query)
    rows = result.all()

    return [
        PipelineWithStatusResponse.from_orm_with_status(pipeline, latest_status)
        for pipeline, latest_status in rows
    ]


@app.post("/api/pipelines/{pipeline_id}/trigger", response_model=TriggerResponse, status_code=202)
async def trigger_pipeline(
    pipeline_id: int,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
) -> TriggerResponse:
    result = await db.execute(select(Pipeline).where(Pipeline.id == pipeline_id))
    pipeline = result.scalar_one_or_none()
    if pipeline is None:
        raise HTTPException(status_code=404, detail=f"Pipeline with id {pipeline_id} not found")

    job_run = JobRun(pipeline_id=pipeline_id, status="PENDING")
    db.add(job_run)
    await db.commit()
    await db.refresh(job_run)

    background_tasks.add_task(run_pipeline_job, pipeline_id, job_run.id)

    return TriggerResponse(job_id=job_run.id, pipeline_id=pipeline_id, status=job_run.status)


@app.get("/api/history", response_model=list[HistoryResponse])
async def get_history(
    db: AsyncSession = Depends(get_db),
) -> list[HistoryResponse]:
    query = (
        select(JobRun, Pipeline)
        .join(Pipeline, JobRun.pipeline_id == Pipeline.id)
        .order_by(JobRun.started_at.desc().nulls_last(), JobRun.id.desc())
        .limit(30)
    )
    result = await db.execute(query)
    rows = result.all()

    return [
        HistoryResponse.from_job_and_pipeline(job_run, pipeline)
        for job_run, pipeline in rows
    ]
