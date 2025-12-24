from __future__ import annotations

import os
import time
import random
from multiprocessing import Process, Event

def worker(node_id: int, start: Event, rounds: int = 12) -> None:
    pid = os.getpid()

    start.wait()

    t0_wall = time.time()
    t0_mono = time.monotonic()

    for i in range(rounds):
        now_wall = time.time()
        now_mono = time.monotonic()

        wall_since = now_wall - t0_wall
        mono_since = now_mono - t0_mono

        print(
            f"[node {node_id} | pid {pid}] "
            f"iter={i:02d} "
            f"wall={now_wall:.6f} (Δ={wall_since:.6f}) "
            f"mono={now_mono:.6f} (Δ={mono_since:.6f}) "
        )

        time.sleep(0.05 + random.random() * 0.15)

def main() -> None:
    start = Event()
    procs: list[Process] = []

    for node_id in range(3):
        p = Process(target=worker, args=(node_id, start))
        p.start()
        procs.append(p)

    time.sleep(0.5)

    print("\n[main] Releasing all nodes at (approximately) the same time...\n")
    start.set()

    for p in procs:
        p.join()

    print("\n[main] Done. Notice: output ordering != real-time ordering.\n")

if __name__ == "__main__":
    main()