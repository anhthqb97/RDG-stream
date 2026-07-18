#!/usr/bin/env python3
"""Mock producer stub — full Kafka publishing in Phase 6."""

from __future__ import annotations

import logging
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("mock-producer")

if __name__ == "__main__":
    log.info("mock-producer stub running (Phase 6 will add Kafka publishing)")
    while True:
        time.sleep(30)