"""Massive S3 Flat Files planning, download, and conversion."""

from ame_stocks_api.flatfiles.convert import (
    CoverageResult,
    FlatFileConvertResult,
    build_daily_coverage,
    convert_flat_file,
)
from ame_stocks_api.flatfiles.massive import (
    MASSIVE_FLAT_FILES_BUCKET,
    MASSIVE_FLAT_FILES_ENDPOINT,
    FlatFileDownloadError,
    FlatFileDownloadResult,
    MassiveFlatFileDownloader,
)
from ame_stocks_api.flatfiles.plan import (
    FlatFileDataset,
    FlatFileObject,
    FlatFilePlan,
    build_flat_file_plan,
)

__all__ = [
    "MASSIVE_FLAT_FILES_BUCKET",
    "MASSIVE_FLAT_FILES_ENDPOINT",
    "CoverageResult",
    "FlatFileConvertResult",
    "FlatFileDataset",
    "FlatFileDownloadError",
    "FlatFileDownloadResult",
    "FlatFileObject",
    "FlatFilePlan",
    "MassiveFlatFileDownloader",
    "build_daily_coverage",
    "build_flat_file_plan",
    "convert_flat_file",
]
