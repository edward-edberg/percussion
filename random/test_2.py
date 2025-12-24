# day2_timestamp_lies_reorder.py
# Goal: Show that send order + timestamps do NOT guarantee receive order.
# This version GUARANTEES reordering is possible by delivering messages concurrently.
#
# Run: python day2_timestamp_lies_reorder.py

from __future__ import annotations

import time
import random
import threading
import multiprocessing as mp
from dataclasses import dataclass


@dataclass
class Message:
    sender: str
    seq: int
    sent_ts: float


def sender(out_q: mp.Queue, n: int = 10, gap_s: float = 0.03) -> None:
    """
    Sends messages in strict increasing order.
    """
    for i in range(n):
        msg = Message(sender="A", seq=i, sent_ts=time.time())
        print(f"[sender] send seq={msg.seq:02d} sent_ts={msg.sent_ts:.6f}")
        out_q.put(msg)
        time.sleep(gap_s)

    # Signal end of stream
    out_q.put(None)


def network(in_q: mp.Queue, out_q: mp.Queue, delay_min: float = 0.0, delay_max: float = 0.30) -> None:
    """
    Simulated network that can REORDER messages.

    Key idea: each message is delivered by its own thread with its own delay,
    so later messages can arrive earlier (reordering).
    """
    threads: list[threading.Thread] = []

    def deliver(msg: Message) -> None:
        delay = random.uniform(delay_min, delay_max)
        time.sleep(delay)
        out_q.put((msg, delay))

    while True:
        msg = in_q.get()
        if msg is None:
            # Stop intake; wait for all in-flight deliveries to finish
            for t in threads:
                t.join()

            # Now it is safe to tell receiver "done"
            out_q.put(None)
            break

        t = threading.Thread(target=deliver, args=(msg,), daemon=False)
        t.start()
        threads.append(t)


def receiver(in_q: mp.Queue) -> None:
    """
    Receives messages and prints arrival order.
    """
    print("\n[receiver] receiving messages...\n")

    arrival_index = 0
    arrival_seqs: list[int] = []

    while True:
        item = in_q.get()
        if item is None:
            break

        msg, delay = item
        recv_ts = time.time()
        arrival_seqs.append(msg.seq)

        print(
            f"[receiver] #{arrival_index:02d} got seq={msg.seq:02d} "
            f"sent_ts={msg.sent_ts:.6f} recv_ts={recv_ts:.6f} "
            f"net_delay={delay:.3f}s"
        )
        arrival_index += 1

    # Summary: did we see reordering?
    in_order = all(arrival_seqs[i] <= arrival_seqs[i + 1] for i in range(len(arrival_seqs) - 1))
    print("\n[receiver] arrival seq order:", arrival_seqs)
    print("[receiver] in-order?" , in_order)
    if not in_order:
        print("[receiver] ✅ Reordering observed (this is the point).")
    else:
        print("[receiver] ⚠️ This run happened to be in-order. Re-run to see reordering (random delays).")


def main() -> None:
    mp.set_start_method("spawn", force=True)  # safer across platforms (macOS, etc.)

    q_in = mp.Queue()
    q_out = mp.Queue()

    p_sender = mp.Process(target=sender, args=(q_in, 12, 0.02))
    p_net = mp.Process(target=network, args=(q_in, q_out, 0.0, 0.35))
    p_recv = mp.Process(target=receiver, args=(q_out,))

    p_recv.start()
    p_net.start()
    p_sender.start()

    p_sender.join()
    p_net.join()
    p_recv.join()

    print("\n[main] Done.\n")


if __name__ == "__main__":
    main()
