from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from dataclasses import dataclass
import random, datetime, uuid, hashlib
from typing import Literal, assert_never
import uvicorn

app = FastAPI(title="Mock Export Server")

# --- Spec Definition ---
EventType = Literal["heart_rate", "spo2", "bp_sys", "bp_dia"]


@dataclass(frozen=True)
class ExportSpec:
    """
    Specification for a synthetic export.
    """

    min_rows: int
    max_rows: int
    event_types: list[EventType]
    downloads: int
    patient_pool: list[str]
    start_time: datetime.datetime
    step: datetime.timedelta


# --- Value Generator Helper ---
def normal_val(
    rng: random.Random,
    mean: float,
    stddev: float,
    min_val: int,
    max_val: int,
) -> int:
    """Generate a random integer from a normal distribution, clamped to bounds."""
    val = int(rng.gauss(mean, stddev))
    return max(min_val, min(max_val, val))


# --- Internal Data Model ---
class DownloadMeta(BaseModel):
    """Internal metadata describing a single download within an export."""

    id: uuid.UUID
    rows: int
    event_types: list[EventType]
    patients: list[str]
    start_time: datetime.datetime
    end_time: datetime.datetime
    step: datetime.timedelta

    def csv(self, chunk_limit: int = 32 * 1024):
        """
        Yield CSV rows spaced by `step` with jitter in [0, step).
        """
        rng = random.Random(self.id.bytes)
        yield "patient_id,event_time,event_type,value\n"

        rows_buf: list[str] = []
        size = 0
        curr_time = self.start_time
        step_seconds = self.step.total_seconds()

        for _ in range(self.rows):
            patient_id = rng.choice(self.patients)
            ts = curr_time + datetime.timedelta(seconds=rng.uniform(0, step_seconds))
            event_type: EventType = rng.choice(self.event_types)

            match event_type:
                case "heart_rate":
                    val = normal_val(rng, mean=75, stddev=15, min_val=30, max_val=200)
                case "spo2":
                    val = normal_val(rng, mean=97, stddev=2, min_val=70, max_val=100)
                case "bp_sys":
                    val = normal_val(rng, mean=120, stddev=20, min_val=60, max_val=250)
                case "bp_dia":
                    val = normal_val(rng, mean=80, stddev=15, min_val=30, max_val=150)
                case _:
                    assert_never(event_type)

            line = f"{patient_id},{ts.isoformat()}Z,{event_type},{val}\n"
            rows_buf.append(line)
            size += len(line)
            curr_time += self.step

            if size >= chunk_limit:
                yield "".join(rows_buf)
                rows_buf.clear()
                size = 0

        if rows_buf:
            yield "".join(rows_buf)


class ExportMeta(BaseModel):
    """Metadata describing an export and its available downloads."""

    id: str
    downloads: dict[uuid.UUID, DownloadMeta]


# --- Seeded UUID4 Helper ---
def seeded_uuid4(seed_str: str) -> uuid.UUID:
    """Generate a deterministic UUID4-like value from a seed string."""
    h = hashlib.sha256(seed_str.encode()).digest()
    return uuid.UUID(bytes=h[:16], version=4)


# --- Registry Builder ---
def build_exports(base: dict[str, ExportSpec]) -> dict[str, ExportMeta]:
    """
    Build the export registry from a dictionary of export specifications.
    Each download gets sequential non-overlapping time windows based on step.
    """
    table: dict[str, ExportMeta] = {}

    for export_id, spec in base.items():
        downloads: dict[uuid.UUID, DownloadMeta] = {}
        rng = random.Random(export_id)

        curr_time = spec.start_time
        step = spec.step

        for i in range(spec.downloads):
            id = seeded_uuid4(f"{export_id}_{i}")
            rows = rng.randint(spec.min_rows, spec.max_rows)
            patients = rng.sample(
                spec.patient_pool,
                k=min(len(spec.patient_pool), rng.randint(2, len(spec.patient_pool))),
            )

            end_time = curr_time + rows * step

            downloads[id] = DownloadMeta(
                id=id,
                rows=rows,
                event_types=list(spec.event_types),
                patients=patients,
                start_time=curr_time,
                end_time=end_time,
                step=step,
            )

            # advance time window for next download
            curr_time = end_time

        table[export_id] = ExportMeta(id=export_id, downloads=downloads)

    return table


EXPORTS = build_exports(
    {
        "demo": ExportSpec(
            min_rows=5_000,
            max_rows=10_000,
            event_types=["bp_sys", "bp_dia"],
            downloads=2,
            patient_pool=["P001", "P002", "P003", "P004"],
            start_time=datetime.datetime(2025, 8, 26, 0, 0, 0),
            step=datetime.timedelta(seconds=7),
        ),
        "small": ExportSpec(
            min_rows=500_000,
            max_rows=1_000_000,
            event_types=["heart_rate", "spo2"],
            downloads=10,
            patient_pool=["S001", "S002", "S003", "S004", "S005", "S006"],
            start_time=datetime.datetime(2025, 8, 26, 0, 0, 0),
            step=datetime.timedelta(seconds=3),
        ),
        "large": ExportSpec(
            min_rows=5_000_000,
            max_rows=10_000_000,
            event_types=["heart_rate", "spo2", "bp_sys", "bp_dia"],
            downloads=20,
            patient_pool=[f"L{i:03d}" for i in range(1, 21)],
            start_time=datetime.datetime(2025, 8, 26, 0, 0, 0),
            step=datetime.timedelta(milliseconds=300),
        ),
    }
)


# --- Generic API Response Wrapper ---
class ApiResponse[T](BaseModel):
    """Wrapper model to standardize API responses under a `data` key."""

    data: T


# --- Response Models ---
class ExportListData(BaseModel):
    """Response model listing all available export IDs."""

    export_ids: list[str]


class ExportDetailData(BaseModel):
    """Response model for details of a specific export, including download IDs only."""

    id: str
    download_ids: list[uuid.UUID]


class DownloadData(BaseModel):
    """Response model for a single download exposed via the API."""

    id: uuid.UUID
    rows: int
    event_types: list[EventType]
    patients: list[str]
    start_time: datetime.datetime
    end_time: datetime.datetime


# --- Endpoints ---
@app.get("/api/export", response_model=ApiResponse[ExportListData])
def list_export() -> ApiResponse[ExportListData]:
    """Return the list of available export IDs."""
    return ApiResponse(data=ExportListData(export_ids=list(EXPORTS.keys())))


@app.get("/api/export/{export_id}", response_model=ApiResponse[ExportDetailData])
def get_export_detail(export_id: str) -> ApiResponse[ExportDetailData]:
    """Return details for a specific export, including its download IDs."""
    if export_id not in EXPORTS:
        raise HTTPException(404, "Export not found")

    export = EXPORTS[export_id]
    return ApiResponse(
        data=ExportDetailData(
            id=export.id,
            download_ids=list(export.downloads.keys()),
        )
    )


@app.get(
    "/api/export/{export_id}/{download_id}", response_model=ApiResponse[DownloadData]
)
def get_download_detail(
    export_id: str, download_id: uuid.UUID
) -> ApiResponse[DownloadData]:
    """Return metadata for a specific download in an export."""
    if export_id not in EXPORTS:
        raise HTTPException(404, "Export not found")
    if download_id not in EXPORTS[export_id].downloads:
        raise HTTPException(404, "Download not found")

    download = EXPORTS[export_id].downloads[download_id]
    return ApiResponse(
        data=DownloadData(
            id=download.id,
            rows=download.rows,
            event_types=download.event_types,
            patients=download.patients,
            start_time=download.start_time,
            end_time=download.end_time,
        )
    )


@app.get("/api/export/{export_id}/{download_id}/data")
def stream_download_data(export_id: str, download_id: uuid.UUID):
    """Stream CSV data for a specific download."""
    if export_id not in EXPORTS:
        raise HTTPException(404, "Export not found")
    if download_id not in EXPORTS[export_id].downloads:
        raise HTTPException(404, "Download not found")

    data_meta = EXPORTS[export_id].downloads[download_id]
    return StreamingResponse(
        content=data_meta.csv(chunk_limit=32 * 1024), media_type="text/csv"
    )


def main():
    """Run the mock server with Uvicorn."""
    uvicorn.run(
        app="server.main:app",
        host="0.0.0.0",
        workers=10,
        port=8000,
        reload=True,
    )
