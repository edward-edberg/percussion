# day5_fault_injection.py
# Goal: Show how message duplication/loss causes bugs, then fix with idempotency.
# Run:
#   python day5_fault_injection.py bug
#   python day5_fault_injection.py fix

from __future__ import annotations

import time
import random
import threading
import multiprocessing as mp
from dataclasses import dataclass
from typing import Optional, Set
import sys
import uuid


@dataclass(frozen=True)
class Op:
    op_id: str       # unique request id
    delta: int       # e.g., +10 deposit
    attempt: int     # retry attempt number (0,1,2...)


# -------------------------
# Sender: sends operations, retries if no ACK
# -------------------------
def sender(op_q: mp.Queue, ack_q: mp.Queue, n_ops: int = 12, timeout_s: float = 0.25, max_retries: int = 3) -> None:
    """
    Sends deposit ops (delta=+10) with unique op_id.
    Uses timeout-based retries (classic at-least-once).
    """
    ops = [str(uuid.uuid4()) for _ in range(n_ops)]
    pending = {op_id: 0 for op_id in ops}  # op_id -> attempt
    acks: Set[str] = set()

    print(f"[sender] starting; will send {n_ops} ops, each delta=+10, retries up to {max_retries}\n")

    # Kick off by sending all once
    for op_id in ops:
        op = Op(op_id=op_id, delta=10, attempt=0)
        op_q.put(op)
        print(f"[sender] SEND op_id={op_id[:8]} attempt=0 delta=+10")

    last_send_time = {op_id: time.time() for op_id in ops}

    # Keep going until all ACKed
    while len(acks) < n_ops:
        # Read any ACKs available (non-blocking-ish with timeout)
        try:
            ack = ack_q.get(timeout=0.05)
            if ack is None:
                break
            if ack not in acks:
                acks.add(ack)
                print(f"[sender] ACK  op_id={ack[:8]} (acked={len(acks)}/{n_ops})")
        except Exception:
            pass

        # Retry timed-out ones
        now = time.time()
        for op_id in list(pending.keys()):
            if op_id in acks:
                pending.pop(op_id, None)
                continue

            if now - last_send_time[op_id] >= timeout_s:
                attempt = pending[op_id] + 1
                if attempt > max_retries:
                    print(f"[sender] GIVEUP op_id={op_id[:8]} after {max_retries} retries")
                    pending.pop(op_id, None)
                    continue

                pending[op_id] = attempt
                last_send_time[op_id] = now
                op = Op(op_id=op_id, delta=10, attempt=attempt)
                op_q.put(op)
                print(f"[sender] RETRY op_id={op_id[:8]} attempt={attempt} delta=+10")

    # Stop the pipeline
    op_q.put(None)
    print("\n[sender] done\n")


# -------------------------
# Network: fault injection (drop/dup/delay/reorder)
# -------------------------
def network(in_q: mp.Queue, out_q: mp.Queue,
            drop_p: float = 0.15,
            dup_p: float = 0.25,
            delay_min: float = 0.0,
            delay_max: float = 0.35) -> None:
    """
    Simulated network with:
    - drop: message disappears
    - dup: message delivered twice (at-least-once vibe)
    - delay: random latency; concurrent delivery creates reordering
    """
    threads: list[threading.Thread] = []

    def deliver(op: Op, tag: str) -> None:
        delay = random.uniform(delay_min, delay_max)
        time.sleep(delay)
        out_q.put(op)
        # (printing from multiple processes can interleave; still useful)
        print(f"[net]  DELIVER{tag} op_id={op.op_id[:8]} attempt={op.attempt} after {delay:.3f}s")

    while True:
        op = in_q.get()
        if op is None:
            # flush deliveries
            for t in threads:
                t.join()
            out_q.put(None)
            print("[net]  shutdown\n")
            break

        # drop?
        if random.random() < drop_p:
            print(f"[net]  DROP op_id={op.op_id[:8]} attempt={op.attempt}")
            continue

        # normal delivery (concurrent)
        t1 = threading.Thread(target=deliver, args=(op, ""))
        t1.start()
        threads.append(t1)

        # duplicate delivery?
        if random.random() < dup_p:
            t2 = threading.Thread(target=deliver, args=(op, " (DUP)"))
            t2.start()
            threads.append(t2)


# -------------------------
# Receiver: BUG mode (no dedupe) or FIX mode (idempotent)
# -------------------------
def receiver(in_q: mp.Queue, ack_q: mp.Queue, mode: str) -> None:
    """
    Applies deposits to a balance.

    bug mode: applies every delivery (duplicates cause double-apply)
    fix mode: dedupes using op_id (idempotent)
    """
    balance = 0
    seen: Set[str] = set()

    print(f"[recv] starting mode={mode!r}\n")

    while True:
        op = in_q.get()
        if op is None:
            break

        if mode == "fix":
            if op.op_id in seen:
                print(f"[recv] IGNORE duplicate op_id={op.op_id[:8]} attempt={op.attempt} balance={balance}")
                # Still ACK so sender stops retrying
                ack_q.put(op.op_id)
                continue
            seen.add(op.op_id)

        # Apply operation (this is where the bug shows up in bug mode)
        balance += op.delta
        print(f"[recv] APPLY  op_id={op.op_id[:8]} attempt={op.attempt} delta=+10 -> balance={balance}")

        # Send ACK
        ack_q.put(op.op_id)

    # tell sender "no more acks" (optional)
    ack_q.put(None)
    print(f"\n[recv] done mode={mode!r} final_balance={balance} (expected ≈ number_of_unique_ops*10)\n")


def main() -> None:
    mode = "bug"
    if len(sys.argv) >= 2:
        mode = sys.argv[1].strip().lower()
    if mode not in ("bug", "fix"):
        print("Usage: python day5_fault_injection.py [bug|fix]")
        raise SystemExit(2)

    mp.set_start_method("spawn", force=True)

    # Queues:
    # sender -> network -> receiver for operations
    q_ops_to_net = mp.Queue()
    q_ops_to_recv = mp.Queue()

    # receiver -> sender for acknowledgements
    q_acks = mp.Queue()

    # Tune fault injection here
    drop_p = 0.15
    dup_p = 0.25
    delay_min = 0.0
    delay_max = 0.35

    p_sender = mp.Process(target=sender, args=(q_ops_to_net, q_acks, 12, 0.25, 3))
    p_net = mp.Process(target=network, args=(q_ops_to_net, q_ops_to_recv, drop_p, dup_p, delay_min, delay_max))
    p_recv = mp.Process(target=receiver, args=(q_ops_to_recv, q_acks, mode))

    p_recv.start()
    p_net.start()
    p_sender.start()

    p_sender.join()
    p_net.join()
    p_recv.join()

    print("[main] Done.")


if __name__ == "__main__":
    main()
