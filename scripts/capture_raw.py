"""Capture raw Binance depth frames for inspection.

Throwaway probe — not part of the package. Writes each WebSocket frame
verbatim, one per line. No parsing: the point is to see what actually arrives
on the wire before designing anything around it.

    uv run python scripts/capture_raw.py
"""

import asyncio
from pathlib import Path

import websockets

URL = "wss://stream.binance.com:9443/ws/btcusdt@depth"
DURATION_S = 30
OUT = Path("data/binance_btcusdt_depth.jsonl")


async def main() -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    frames = 0

    async with websockets.connect(URL) as ws:
        with OUT.open("w", encoding="utf-8") as f:
            try:
                async with asyncio.timeout(DURATION_S):
                    async for message in ws:
                        f.write(message + "\n")
                        frames += 1
            except TimeoutError:
                pass

    print(f"{frames} frames in {DURATION_S}s -> {OUT}")


if __name__ == "__main__":
    asyncio.run(main())
