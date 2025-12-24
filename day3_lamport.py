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
    lamport: int

def sender(out_q: mp.Queue, n: int = 12, gap_s: float = 0.02) -> None:
    """
    Sender A:
    - maintains Lamport clock LC_A
    - increments before each send
    - attaches lamport timestamp to each message
    """

    lc = 0
    for i in range(n):
        lc += 1
        msg = Message(sender="A", seq=i, lamport=lc)
        print(f"[sender A] send seq={msg.seq:02d} lamport={msg.lamport}")
        out_q.put(msg)
        time.sleep(gap_s)

    out_q.put(None) # end of stream

def network(in_q: mp.Queue, out_q: mp.Queue, delay_min: float = 0.0, delay_max: float = 0.35) -> None:
    """
    Simulated network that can reorder messagevia concurrent delivery threads.
    """
    threads: list[threading.Thread] = []

    def deliver(msg: Message) -> None:
        delay = random.uniform(delay_min, delay_max)
        time.sleep(delay)
        out_q.put((msg, delay))

    while True:
        msg = in_q.get()
        if msg is None:
            for t in threads:
                t.join()

            out_q.put(None)
            break

        t = threading.Thread(target=deliver, args=(msg,), daemon=False) # need tuple (msg,)
        t.start()
        threads.append(t)

def receiver(in_q: mp.Queue) -> None:
    """
    Receiver B:
    - maintains Lamport clock LC_B
    - on receive message with timestamp t:
        LC_B = max(LC_B, t) + 1
    - prints arrival order vs logical time progression
    """

    print("\n[receiver B] receiving messages...")

    lc = 0
    arrival_index = 0
    arrivals: list[tuple[int, int]] = [] # (seq, lamport_from_sender)

    while True:
        item = in_q.get()
        if item is None:
            break

        msg, delay = item

        lc = max(lc, msg.lamport) + 1

        arrivals.append((msg.seq, msg.lamport))

        print(
            f"[receiver B] #{arrival_index:02d} got seq={msg.seq:02d} "
            f"msg_lamport={msg.lamport:02d} "
            f"LC_B_after={lc:02d} "
            f"net_delay={delay:.3f}s"
        )
        arrival_index += 1

    # summary
    print("\n[receiver B] Summary")
    print("Arrival order (seq, msg_lamport):", arrivals)

    # what if we sort by Lamport timestamp (and tie-break by sender, seq)?
    # sorted_by_lamport = sorted(arrivals, key=lambda x: (x[1], x[0]))
    sorted_by_lamport = sorted(arrivals, key=lambda x: (x[0], x[1]))
    print("Sorted by msg_lamport:", sorted_by_lamport)

    print(
        "\nNote: Sorting by Lamport gives a consistent logical ordering label, "
        "but it doesn't prove causalty for all pairs (that's Day 4)"
    )

def main() -> None:
    mp.set_start_method("spawn", force = True) 

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