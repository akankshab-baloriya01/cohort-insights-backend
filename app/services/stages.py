"""Simulated pipeline stages. Deterministic output, random latency and failure."""

import asyncio
import random
import re
from collections import Counter

SUMMARY_WORDS = 40
MAX_TAGS = 5

_STOPWORDS = frozenset(
    """a an and are as at be been but by can could did do does for from had has have he her his i if in into is it
    its me my no not of on or our she so than that the their them then there these they this to too us was we were
    what when which who will with would you your summary words""".split()
)
_WORD_RE = re.compile(r"[a-zA-Z][a-zA-Z'-]{2,}")


class StageFailure(Exception):
    """A simulated, retryable stage failure."""


def mock_summary(content: str) -> str:
    words = content.split()
    head = " ".join(words[:SUMMARY_WORDS])
    suffix = "…" if len(words) > SUMMARY_WORDS else ""
    return f"Summary ({len(words)} words): {head}{suffix}"


def mock_tags(summary: str) -> list[str]:
    words = [w.lower().strip("'-") for w in _WORD_RE.findall(summary)]
    counts = Counter(w for w in words if w not in _STOPWORDS)
    # Stable ordering: frequency desc, then first occurrence.
    first_seen = {w: i for i, w in reversed(list(enumerate(words)))}
    ranked = sorted(counts, key=lambda w: (-counts[w], first_seen[w]))
    return ranked[:MAX_TAGS]


class StageExecutor:
    def __init__(
        self,
        processing_range: tuple[float, float],
        enriching_range: tuple[float, float],
        failure_rate: float,
        rng: random.Random | None = None,
    ):
        self.processing_range = processing_range
        self.enriching_range = enriching_range
        self.failure_rate = failure_rate
        self.rng = rng or random.Random()

    async def _simulate(self, bounds: tuple[float, float], stage: str) -> None:
        low, high = bounds
        await asyncio.sleep(self.rng.uniform(low, max(low, high)))
        if self.rng.random() < self.failure_rate:
            raise StageFailure(f"simulated {stage} failure")

    async def summarize(self, content: str) -> str:
        await self._simulate(self.processing_range, "processing")
        return mock_summary(content)

    async def enrich(self, summary: str) -> list[str]:
        await self._simulate(self.enriching_range, "enriching")
        return mock_tags(summary)
