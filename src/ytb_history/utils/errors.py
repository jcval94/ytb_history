"""Custom errors."""

from __future__ import annotations


class PipelineError(Exception):
    """Base pipeline exception."""


class MissingYouTubeApiKeyError(PipelineError):
    """Raised when no YouTube API key is configured."""


class YouTubeApiError(PipelineError):
    """Raised for non-retryable YouTube API failures."""


class YouTubeRetryableApiError(YouTubeApiError):
    """Raised when a retryable YouTube API failure exhausts retries."""
