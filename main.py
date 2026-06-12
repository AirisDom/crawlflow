import json
from contextlib import asynccontextmanager
from datetime import datetime
from collections.abc import AsyncGenerator
from typing import Literal

from fastapi import Depends, FastAPI
from pydantic import BaseModel, HttpUrl, field_validator
from sqlalchemy import ForeignKey, Text, event, select, func
from sqlalchemy.ext.asyncio import AsyncAttrs, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

DATABASE_URL = "sqlite+aiosqlite:///./crawlflow.db"

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
