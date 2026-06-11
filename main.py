import json
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Literal

from fastapi import FastAPI
from pydantic import BaseModel, HttpUrl, field_validator
from sqlalchemy import ForeignKey, Text, event
from sqlalchemy.ext.asyncio import AsyncAttrs, async_sessionmaker, create_async_engine
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


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}
