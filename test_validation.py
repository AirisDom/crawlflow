import pytest
from pydantic import ValidationError
from httpx import AsyncClient, ASGITransport

from main import SelectorConfig, PipelineCreate, app, engine, Base


class TestSelectorConfig:
    def test_valid_selector_text(self):
        s = SelectorConfig(key="title", selector="h1", attribute="text")
        assert s.key == "title"
        assert s.selector == "h1"
        assert s.attribute == "text"

    def test_valid_selector_href(self):
        s = SelectorConfig(key="links", selector="a.nav", attribute="href")
        assert s.attribute == "href"

    def test_valid_selector_src(self):
        s = SelectorConfig(key="images", selector="img.thumb", attribute="src")
        assert s.attribute == "src"

    def test_empty_key_rejected(self):
        with pytest.raises(ValidationError) as exc_info:
            SelectorConfig(key="", selector="h1", attribute="text")
        assert "selector key cannot be empty" in str(exc_info.value)

    def test_whitespace_key_rejected(self):
        with pytest.raises(ValidationError) as exc_info:
            SelectorConfig(key="   ", selector="h1", attribute="text")
        assert "selector key cannot be empty" in str(exc_info.value)

    def test_empty_selector_rejected(self):
        with pytest.raises(ValidationError) as exc_info:
            SelectorConfig(key="title", selector="", attribute="text")
        assert "CSS selector cannot be empty" in str(exc_info.value)

    def test_whitespace_selector_rejected(self):
        with pytest.raises(ValidationError) as exc_info:
            SelectorConfig(key="title", selector="   ", attribute="text")
        assert "CSS selector cannot be empty" in str(exc_info.value)

    def test_invalid_attribute_rejected(self):
        with pytest.raises(ValidationError) as exc_info:
            SelectorConfig(key="title", selector="h1", attribute="class")
        assert "text" in str(exc_info.value) or "href" in str(exc_info.value) or "src" in str(exc_info.value)

    def test_key_stripped(self):
        s = SelectorConfig(key="  title  ", selector="h1", attribute="text")
        assert s.key == "title"

    def test_selector_stripped(self):
        s = SelectorConfig(key="title", selector="  h1.heading  ", attribute="text")
        assert s.selector == "h1.heading"


class TestPipelineCreate:
    def test_valid_pipeline(self):
        p = PipelineCreate(
            name="Test Pipeline",
            target_url="https://example.com",
            selectors=[SelectorConfig(key="title", selector="h1", attribute="text")]
        )
        assert p.name == "Test Pipeline"
        assert str(p.target_url) == "https://example.com/"

    def test_empty_name_rejected(self):
        with pytest.raises(ValidationError) as exc_info:
            PipelineCreate(
                name="",
                target_url="https://example.com",
                selectors=[SelectorConfig(key="title", selector="h1", attribute="text")]
            )
        assert "pipeline name cannot be empty" in str(exc_info.value)

    def test_whitespace_name_rejected(self):
        with pytest.raises(ValidationError) as exc_info:
            PipelineCreate(
                name="   ",
                target_url="https://example.com",
                selectors=[SelectorConfig(key="title", selector="h1", attribute="text")]
            )
        assert "pipeline name cannot be empty" in str(exc_info.value)

    def test_name_stripped(self):
        p = PipelineCreate(
            name="  Test Pipeline  ",
            target_url="https://example.com",
            selectors=[SelectorConfig(key="title", selector="h1", attribute="text")]
        )
        assert p.name == "Test Pipeline"

    def test_long_name_rejected(self):
        long_name = "a" * 256
        with pytest.raises(ValidationError) as exc_info:
            PipelineCreate(
                name=long_name,
                target_url="https://example.com",
                selectors=[SelectorConfig(key="title", selector="h1", attribute="text")]
            )
        assert "cannot exceed 255 characters" in str(exc_info.value)

    def test_empty_url_rejected(self):
        with pytest.raises(ValidationError) as exc_info:
            PipelineCreate(
                name="Test",
                target_url="",
                selectors=[SelectorConfig(key="title", selector="h1", attribute="text")]
            )
        assert "target URL cannot be empty" in str(exc_info.value)

    def test_whitespace_url_rejected(self):
        with pytest.raises(ValidationError) as exc_info:
            PipelineCreate(
                name="Test",
                target_url="   ",
                selectors=[SelectorConfig(key="title", selector="h1", attribute="text")]
            )
        assert "target URL cannot be empty" in str(exc_info.value)

    def test_non_http_url_rejected(self):
        with pytest.raises(ValidationError) as exc_info:
            PipelineCreate(
                name="Test",
                target_url="ftp://example.com",
                selectors=[SelectorConfig(key="title", selector="h1", attribute="text")]
            )
        assert "must start with http://" in str(exc_info.value)

    def test_invalid_url_format_rejected(self):
        with pytest.raises(ValidationError) as exc_info:
            PipelineCreate(
                name="Test",
                target_url="not-a-url",
                selectors=[SelectorConfig(key="title", selector="h1", attribute="text")]
            )
        assert "must start with http://" in str(exc_info.value)

    def test_http_url_accepted(self):
        p = PipelineCreate(
            name="Test",
            target_url="http://example.com",
            selectors=[SelectorConfig(key="title", selector="h1", attribute="text")]
        )
        assert "http://example.com" in str(p.target_url)

    def test_https_url_accepted(self):
        p = PipelineCreate(
            name="Test",
            target_url="https://example.com/path?query=1",
            selectors=[SelectorConfig(key="title", selector="h1", attribute="text")]
        )
        assert "https://example.com" in str(p.target_url)

    def test_empty_selectors_rejected(self):
        with pytest.raises(ValidationError) as exc_info:
            PipelineCreate(
                name="Test",
                target_url="https://example.com",
                selectors=[]
            )
        assert "at least one selector is required" in str(exc_info.value)

    def test_duplicate_selector_keys_rejected(self):
        with pytest.raises(ValidationError) as exc_info:
            PipelineCreate(
                name="Test",
                target_url="https://example.com",
                selectors=[
                    SelectorConfig(key="title", selector="h1", attribute="text"),
                    SelectorConfig(key="title", selector="h2", attribute="text"),
                ]
            )
        assert "duplicate selector keys found" in str(exc_info.value)
        assert "title" in str(exc_info.value)

    def test_multiple_duplicate_keys_reported(self):
        with pytest.raises(ValidationError) as exc_info:
            PipelineCreate(
                name="Test",
                target_url="https://example.com",
                selectors=[
                    SelectorConfig(key="title", selector="h1", attribute="text"),
                    SelectorConfig(key="link", selector="a", attribute="href"),
                    SelectorConfig(key="title", selector="h2", attribute="text"),
                    SelectorConfig(key="link", selector="a.nav", attribute="href"),
                ]
            )
        error_str = str(exc_info.value)
        assert "duplicate selector keys found" in error_str
        assert "title" in error_str
        assert "link" in error_str

    def test_unique_selector_keys_accepted(self):
        p = PipelineCreate(
            name="Test",
            target_url="https://example.com",
            selectors=[
                SelectorConfig(key="title", selector="h1", attribute="text"),
                SelectorConfig(key="links", selector="a", attribute="href"),
                SelectorConfig(key="images", selector="img", attribute="src"),
            ]
        )
        assert len(p.selectors) == 3

    def test_selectors_to_json(self):
        p = PipelineCreate(
            name="Test",
            target_url="https://example.com",
            selectors=[SelectorConfig(key="title", selector="h1", attribute="text")]
        )
        json_str = p.selectors_to_json()
        assert "title" in json_str
        assert "h1" in json_str
        assert "text" in json_str


async def setup_test_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def teardown_test_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


async def get_test_client():
    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport, base_url="http://test")


@pytest.mark.asyncio
async def test_api_422_on_empty_name():
    await setup_test_db()
    try:
        async with await get_test_client() as client:
            response = await client.post("/api/pipelines", json={
                "name": "",
                "target_url": "https://example.com",
                "selectors": [{"key": "title", "selector": "h1", "attribute": "text"}]
            })
            assert response.status_code == 422
            data = response.json()
            assert "detail" in data
            detail_str = str(data["detail"])
            assert "pipeline name cannot be empty" in detail_str
    finally:
        await teardown_test_db()


@pytest.mark.asyncio
async def test_api_422_on_invalid_url():
    await setup_test_db()
    try:
        async with await get_test_client() as client:
            response = await client.post("/api/pipelines", json={
                "name": "Test",
                "target_url": "not-a-url",
                "selectors": [{"key": "title", "selector": "h1", "attribute": "text"}]
            })
            assert response.status_code == 422
            data = response.json()
            assert "detail" in data
            detail_str = str(data["detail"])
            assert "must start with http://" in detail_str
    finally:
        await teardown_test_db()


@pytest.mark.asyncio
async def test_api_422_on_ftp_url():
    await setup_test_db()
    try:
        async with await get_test_client() as client:
            response = await client.post("/api/pipelines", json={
                "name": "Test",
                "target_url": "ftp://example.com",
                "selectors": [{"key": "title", "selector": "h1", "attribute": "text"}]
            })
            assert response.status_code == 422
    finally:
        await teardown_test_db()


@pytest.mark.asyncio
async def test_api_422_on_empty_selectors():
    await setup_test_db()
    try:
        async with await get_test_client() as client:
            response = await client.post("/api/pipelines", json={
                "name": "Test",
                "target_url": "https://example.com",
                "selectors": []
            })
            assert response.status_code == 422
            data = response.json()
            assert "detail" in data
            detail_str = str(data["detail"])
            assert "at least one selector is required" in detail_str
    finally:
        await teardown_test_db()


@pytest.mark.asyncio
async def test_api_422_on_invalid_attribute():
    await setup_test_db()
    try:
        async with await get_test_client() as client:
            response = await client.post("/api/pipelines", json={
                "name": "Test",
                "target_url": "https://example.com",
                "selectors": [{"key": "title", "selector": "h1", "attribute": "class"}]
            })
            assert response.status_code == 422
    finally:
        await teardown_test_db()


@pytest.mark.asyncio
async def test_api_422_on_empty_selector_key():
    await setup_test_db()
    try:
        async with await get_test_client() as client:
            response = await client.post("/api/pipelines", json={
                "name": "Test",
                "target_url": "https://example.com",
                "selectors": [{"key": "", "selector": "h1", "attribute": "text"}]
            })
            assert response.status_code == 422
            data = response.json()
            detail_str = str(data["detail"])
            assert "selector key cannot be empty" in detail_str
    finally:
        await teardown_test_db()


@pytest.mark.asyncio
async def test_api_422_on_empty_css_selector():
    await setup_test_db()
    try:
        async with await get_test_client() as client:
            response = await client.post("/api/pipelines", json={
                "name": "Test",
                "target_url": "https://example.com",
                "selectors": [{"key": "title", "selector": "", "attribute": "text"}]
            })
            assert response.status_code == 422
            data = response.json()
            detail_str = str(data["detail"])
            assert "CSS selector cannot be empty" in detail_str
    finally:
        await teardown_test_db()


@pytest.mark.asyncio
async def test_api_422_on_duplicate_selector_keys():
    await setup_test_db()
    try:
        async with await get_test_client() as client:
            response = await client.post("/api/pipelines", json={
                "name": "Test",
                "target_url": "https://example.com",
                "selectors": [
                    {"key": "title", "selector": "h1", "attribute": "text"},
                    {"key": "title", "selector": "h2", "attribute": "text"},
                ]
            })
            assert response.status_code == 422
            data = response.json()
            detail_str = str(data["detail"])
            assert "duplicate selector keys found" in detail_str
    finally:
        await teardown_test_db()


@pytest.mark.asyncio
async def test_api_409_on_duplicate_pipeline_name():
    await setup_test_db()
    try:
        async with await get_test_client() as client:
            first_response = await client.post("/api/pipelines", json={
                "name": "UniqueTest",
                "target_url": "https://example.com",
                "selectors": [{"key": "title", "selector": "h1", "attribute": "text"}]
            })
            assert first_response.status_code == 201

            second_response = await client.post("/api/pipelines", json={
                "name": "UniqueTest",
                "target_url": "https://other.com",
                "selectors": [{"key": "link", "selector": "a", "attribute": "href"}]
            })
            assert second_response.status_code == 409
            data = second_response.json()
            assert "detail" in data
            assert "already exists" in data["detail"]
            assert "UniqueTest" in data["detail"]
    finally:
        await teardown_test_db()


@pytest.mark.asyncio
async def test_api_valid_pipeline_creation():
    await setup_test_db()
    try:
        async with await get_test_client() as client:
            response = await client.post("/api/pipelines", json={
                "name": "Valid Pipeline",
                "target_url": "https://example.com/page",
                "selectors": [
                    {"key": "title", "selector": "h1", "attribute": "text"},
                    {"key": "links", "selector": "a.nav", "attribute": "href"},
                ]
            })
            assert response.status_code == 201
            data = response.json()
            assert data["name"] == "Valid Pipeline"
            assert "example.com" in data["target_url"]
            assert len(data["selectors"]) == 2
    finally:
        await teardown_test_db()


@pytest.mark.asyncio
async def test_api_delete_pipeline_success():
    await setup_test_db()
    try:
        async with await get_test_client() as client:
            create_response = await client.post("/api/pipelines", json={
                "name": "ToDelete",
                "target_url": "https://example.com",
                "selectors": [{"key": "title", "selector": "h1", "attribute": "text"}]
            })
            assert create_response.status_code == 201
            pipeline_id = create_response.json()["id"]

            delete_response = await client.delete(f"/api/pipelines/{pipeline_id}")
            assert delete_response.status_code == 204

            list_response = await client.get("/api/pipelines")
            assert list_response.status_code == 200
            pipelines = list_response.json()
            pipeline_ids = [p["id"] for p in pipelines]
            assert pipeline_id not in pipeline_ids
    finally:
        await teardown_test_db()


@pytest.mark.asyncio
async def test_api_delete_pipeline_not_found():
    await setup_test_db()
    try:
        async with await get_test_client() as client:
            response = await client.delete("/api/pipelines/99999")
            assert response.status_code == 404
            data = response.json()
            assert "detail" in data
            assert "99999" in data["detail"]
    finally:
        await teardown_test_db()


@pytest.mark.asyncio
async def test_api_delete_pipeline_cascades_job_runs():
    await setup_test_db()
    try:
        async with await get_test_client() as client:
            create_response = await client.post("/api/pipelines", json={
                "name": "ToDeleteWithJobs",
                "target_url": "https://httpbin.org/html",
                "selectors": [{"key": "title", "selector": "h1", "attribute": "text"}]
            })
            assert create_response.status_code == 201
            pipeline_id = create_response.json()["id"]

            trigger_response = await client.post(f"/api/pipelines/{pipeline_id}/trigger")
            assert trigger_response.status_code == 202
            job_id = trigger_response.json()["job_id"]

            delete_response = await client.delete(f"/api/pipelines/{pipeline_id}")
            assert delete_response.status_code == 204

            history_response = await client.get("/api/history")
            assert history_response.status_code == 200
            history = history_response.json()
            job_ids = [h["id"] for h in history]
            assert job_id not in job_ids
    finally:
        await teardown_test_db()
