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
                <div class="space-y-4">
                    <p class="text-gray-400">Pipeline configuration form will be rendered here.</p>
                </div>
            </section>

            <section id="orchestration-matrix" class="bg-gray-800 rounded-lg p-6 shadow-lg">
                <h2 class="text-xl font-semibold text-white mb-4 border-b border-gray-700 pb-2">Orchestration Matrix</h2>
                <div class="space-y-6">
                    <div id="pipeline-grid">
                        <h3 class="text-lg font-medium text-gray-300 mb-3">Active Pipelines</h3>
                        <p class="text-gray-400">Pipeline tiles will be rendered here.</p>
                    </div>
                    <div id="history-timeline">
                        <h3 class="text-lg font-medium text-gray-300 mb-3">Execution History</h3>
                        <p class="text-gray-400">Job history timeline will be rendered here.</p>
                    </div>
                </div>
            </section>
        </div>
    </div>
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
