
# day4_vector_clocks.py
# Goal: Use vector clocks to detect concurrency (not just assign an order).
# Run: python day4_vector_clocks.py

from __future__ import annotations

import time
import random
import threading
import multiprocessing as mp
from dataclasses import dataclass
from typing import List, Tuple, Optional


# We'll use fixed node ordering: A=0, B=1
A, B = 0, 1
NODE_NAMES = {A: "A", B: "B"}


def vc_leq(v: List[int], w: List[int]) -> bool:
    """Elementwise v <= w."""
    return all(v[i] <= w[i] for i in range(len(v)))


def vc_concurrent(v: List[int], w: List[int]) -> bool:
    """True if neither v<=w nor w<=v."""
    return (not vc_leq(v, w)) and (not vc_leq(w, v))


def vc_max(v: List[int], w: List[int]) -> List[int]:
    """Elementwise max."""
    return [max(v[i], w[i]) for i in range(len(v))]


@dataclass
class Message:
    sender: int           # 0 for A, 1 for B
    receiver: int         # 0 for A, 1 for B
    seq: int              # per-sender sequence
    vc: List[int]         # vector clock snapshot at send time


def node_process(node_id: int, out_q: mp.Queue, in_q: mp.Queue, n_sends: int = 6) -> None:
    """
    Each node:
    - does a few local events
    - sends messages to the other node
    - receives messages and updates vector clock
    """
    other = B if node_id == A else A

    vc = [0, 0]  # [count_A, count_B]

    def local_event(tag: str) -> None:
        vc[node_id] += 1
        print(f"[node {NODE_NAMES[node_id]}] {tag} local_event -> VC={vc}")

    def send_one(seq: int) -> None:
        # Send is also an event
        vc[node_id] += 1
        msg = Message(sender=node_id, receiver=other, seq=seq, vc=vc.copy())
        print(f"[node {NODE_NAMES[node_id]}] SEND to {NODE_NAMES[other]} seq={seq:02d} msg.VC={msg.vc}")
        out_q.put(msg)

    # Do some local work + sends interleaved
    for i in range(n_sends):
        local_event(tag=f"step{i:02d}")
        send_one(i)
        time.sleep(0.02 + random.random() * 0.05)

    # Signal that this node is done sending
    out_q.put(Message(sender=node_id, receiver=-1, seq=-1, vc=[]))  # "done" marker

    # Receive until we get "done" from network for this node
    while True:
        item = in_q.get()
        if item is None:
            break

        msg: Message = item

        # Receive rule: merge + increment self
        before = vc.copy()
        vc = vc_max(vc, msg.vc)
        vc[node_id] += 1

        relation = "UNKNOWN"
        if vc_leq(msg.vc, vc):
            relation = "msg -> recv (causal or equal)"

        print(
            f"[node {NODE_NAMES[node_id]}] RECV from {NODE_NAMES[msg.sender]} seq={msg.seq:02d} "
            f"msg.VC={msg.vc}  before={before}  after={vc}  ({relation})"
        )

    print(f"[node {NODE_NAMES[node_id]}] DONE final VC={vc}")


def network(in_q: mp.Queue, out_q_A: mp.Queue, out_q_B: mp.Queue,
            delay_min: float = 0.0, delay_max: float = 0.35) -> None:
    """
    Network:
    - takes messages from both nodes via in_q
    - delivers them to the correct node's incoming queue with independent random delay
    - stops when it has seen "done" markers from both senders and all in-flight deliveries finish
    """
    done = {A: False, B: False}
    threads: list[threading.Thread] = []

    def deliver(msg: Message) -> None:
        delay = random.uniform(delay_min, delay_max)
        time.sleep(delay)
        if msg.receiver == A:
            out_q_A.put(msg)
        elif msg.receiver == B:
            out_q_B.put(msg)

    while True:
        msg: Message = in_q.get()

        # done marker: receiver == -1
        if msg.receiver == -1:
            done[msg.sender] = True
            if done[A] and done[B]:
                # stop intake; flush deliveries; then close both receivers
                for t in threads:
                    t.join()
                out_q_A.put(None)
                out_q_B.put(None)
                break
            continue

        t = threading.Thread(target=deliver, args=(msg,), daemon=False)
        t.start()
        threads.append(t)


def main() -> None:
    mp.set_start_method("spawn", force=True)

    # One shared outgoing queue into the network
    q_to_net = mp.Queue()

    # Separate incoming queues for each node
    q_to_A = mp.Queue()
    q_to_B = mp.Queue()

    p_net = mp.Process(target=network, args=(q_to_net, q_to_A, q_to_B))
    p_A = mp.Process(target=node_process, args=(A, q_to_net, q_to_A))
    p_B = mp.Process(target=node_process, args=(B, q_to_net, q_to_B))

    p_net.start()
    p_A.start()
    p_B.start()

    p_A.join()
    p_B.join()
    p_net.join()

    print("\n[main] Done.\n")


if __name__ == "__main__":
    main()
