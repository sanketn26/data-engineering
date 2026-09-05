"""Deterministic model of downstream watermark minimum and source idleness."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class SourceSplit:
    watermark: int
    last_activity: int

    def active_at(self, now: int, idle_after: Optional[int]) -> bool:
        return idle_after is None or now - self.last_activity < idle_after


def downstream_watermark(splits, now, idle_after=None):
    active = [split.watermark for split in splits if split.active_at(now, idle_after)]
    return min(active) if active else None


def main():
    fresh = SourceSplit(watermark=119_995, last_activity=120)
    silent = SourceSplit(watermark=3_600, last_activity=60)
    splits = [fresh, silent]

    stalled = downstream_watermark(splits, now=120)
    recovered = downstream_watermark(splits, now=120, idle_after=30)

    assert stalled == 3_600
    assert recovered == 119_995
    print(f"without idleness: downstream watermark={stalled} (stalled)")
    print(f"with 30s idleness: downstream watermark={recovered} (advancing)")
    print("PASS: idleness excludes the silent split from the downstream minimum")


if __name__ == "__main__":
    main()
