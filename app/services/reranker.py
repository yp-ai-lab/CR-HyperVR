from __future__ import annotations

from functools import lru_cache
from typing import Sequence


class Reranker:
    def rerank(self, query: str, items: Sequence[dict]) -> list[dict]:
        q_tokens = {t for t in query.lower().split() if t}

        def score(it: dict) -> int:
            title = str(it.get("title", "")).lower()
            tokens = set(title.split())
            return len(q_tokens & tokens)

        return sorted(list(items), key=score, reverse=True)


@lru_cache(maxsize=1)
def get_reranker() -> Reranker:
    return Reranker()

