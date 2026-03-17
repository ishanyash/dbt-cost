from __future__ import annotations

from abc import ABC, abstractmethod


class CostAdapter(ABC):
    """Abstract interface for warehouse cost estimation."""

    @abstractmethod
    def dry_run(self, sql: str, project: str, dataset: str) -> int:
        """Estimate bytes processed without executing. Returns bytes."""
        ...

    @abstractmethod
    def adapter_type(self) -> str:
        """Return the adapter type string (e.g., 'bigquery')."""
        ...
