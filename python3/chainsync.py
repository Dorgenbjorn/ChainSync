import sys
import time
import socket
import random
import argparse
import threading


class ChainSyncNode:
    def __init__(self, node_id: str, listen_port: int | None, succ_addr: tuple[str, int] | None, task_func):
        self.node_id = node_id
        self.listen_port = listen_port
        self.succ_addr = succ_addr
        self.task_func = task_func

        self.is_head = listen_port is None
        self.is_tail = succ_addr is None
        self.is_intm = not (self.is_head or self.is_tail)
        self.pred_ready = self.is_head
        self.pred_complete = self.is_head

        self.pred_socket: socket.socket | None = None
        self.succ_socket: socket.socket | None = None
        self.server_socket: socket.socket | None = None

        self.state = "SYNC"  # SYNC, READY, WATCH, START, COMPLETE
        self.local_ready = False
        self.has_ready_from_pred = False
        self.has_complete_from_succ = False
        self.has_start_from_succ = False

        self.lock = threading.Lock()
        self.exit_flag = False

    def message_pred(self, msg: str):
        if self.pred_socket:
            try:
                print(f"[{self.node_id}] sending message {msg} to predecessor")
                self.pred_socket.sendall((msg + "\n").encode("utf-8"))
            except Exception as ex:
                print(f"Exception messaging predecessor: {ex}")

    def message_succ(self, msg: str):
        if self.succ_socket:
            try:
                print(f"[{self.node_id}] sending message {msg} to successor")
                self.succ_socket.sendall((msg + "\n").encode("utf-8"))
            except Exception as ex:
                print(f"Exception messaging successor: {ex}")

    def execute_task(self):
        try:
            self.task_func()
        finally:
            self.task_completed()

    def task_completed(self):
        self.transition_state()
        if not self.is_tail:
            self.message_succ("COMPLETE")
        else:
            self.message_pred("COMPLETE")
            self.exit()

    def become_locally_ready(self):
        if self.local_ready:
            return
        self.local_ready = True
        print(f"[{self.node_id}] locally ready")
        self.transition_state()

    def exit(self):
        print(f"[{self.node_id}] Setting exit flag...")
        self.exit_flag = True

    def transition_state(self):
        start_state = self.state
        if self.state == "SYNC" and self.pred_ready and self.local_ready:
            self.state = "READY"
            print(f"[{self.node_id}] State transition: {start_state} -> {self.state}")
            if self.is_tail:
                self.has_start_from_succ = True # Tail initiates the backward START wave
                self.transition_state()
            else:
                self.message_succ("READY")
        elif self.state == "READY" and self.is_head and self.has_start_from_succ:
            self.state = "START"
            print(f"[{self.node_id}] State transition: {start_state} -> {self.state}")
            self.execute_task()
        elif self.state == "READY" and not self.is_head and self.has_start_from_succ:
            self.state = "WATCH"
            print(f"[{self.node_id}] State transition: {start_state} -> {self.state}")
            self.message_pred("START")
        elif self.state == "WATCH" and self.pred_complete:
            self.state = "START"
            print(f"[{self.node_id}] State transition: {start_state} -> {self.state}")
            self.execute_task()
        elif self.state == "START":
            self.state = "COMPLETE"
            print(f"[{self.node_id}] State transition: {start_state} -> {self.state}")
        else: # do nothing
            print(f"[{self.node_id}] State transition: {start_state} -> {self.state}")
        return None

    def handle_message(self, cmd: str, direction: str):
        print(f"[{self.node_id}] received {cmd} from {direction}")
        if cmd == "READY":
            if direction == "pred":
                self.pred_ready = True
                self.transition_state()
        elif cmd == "START":
            if direction == "succ":
                self.has_start_from_succ = True
                self.transition_state()
        elif cmd == "COMPLETE":
            if direction == "pred":
                self.pred_complete = True
                self.transition_state()
            elif direction == "succ":
                self.message_pred("COMPLETE")
                self.exit()

    def _reader(self, sock: socket.socket, direction: str):
        try:
            f = sock.makefile("r", encoding="utf-8")
            while True:
                line = f.readline()
                if not line:
                    break
                cmd = line.strip()
                if cmd:
                    self.handle_message(cmd, direction)
        except Exception as e:
            print(f"[{self.node_id}] reader error on {direction}: {e}")
        finally:
            sock.close()

    def _accept_pred(self):
        try:
            conn, addr = self.server_socket.accept()
            print(f"[{self.node_id}] predecessor connected from {addr}")
            self.pred_socket = conn
            threading.Thread(target=self._reader, args=(conn, "pred"), daemon=True).start()
        except Exception as e:
            print(f"[{self.node_id}] accept error: {e}")

    def start(self):
        print(f"[{self.node_id}] starting (head={'yes' if self.is_head else 'no'}, tail={'yes' if self.is_tail else 'no'})")
        if not self.is_head:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind(('', self.listen_port))
            self.server_socket.listen(1)
            print(f"[{self.node_id}] listening on port {self.listen_port} for predecessor")
            threading.Thread(target=self._accept_pred, daemon=True).start()

        if not self.is_tail:
            self.succ_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            print(f"[{self.node_id}] connecting to successor {self.succ_addr} ...")
            while True:
                try:
                    self.succ_socket.connect(self.succ_addr)
                    break
                except ConnectionRefusedError:
                    time.sleep(0.5)
            print(f"[{self.node_id}] connected to successor")
            threading.Thread(target=self._reader, args=(self.succ_socket, "succ"), daemon=True).start()
        while (not self.is_head and self.pred_socket is None) or (not self.is_tail and self.succ_socket is None):
            print(f"[{self.node_id}] waiting for connection...")
            time.sleep(0.5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ChainSync reference implementation")
    parser.add_argument("--id", required=True, help="Node identifier (A, B, C, ...)")
    parser.add_argument("--listen-port", type=int, help="Port to listen on for predecessor (omit for head)")
    parser.add_argument("--succ-host", default="localhost", help="Successor host (default: localhost)")
    parser.add_argument("--succ-port", type=int, help="Successor port (omit for tail)")
    parser.add_argument("--task-duration", type=float, default=None, help="Task duration in seconds (default: random 1-8s)")
    #parser.add_argument("--start-in-complete", default=False, help="The node start in the 'COMPLETE' state. This option is useful if the server must restart.")

    args = parser.parse_args()

    succ_addr = (args.succ_host, args.succ_port) if args.succ_port else None

    duration = args.task_duration if args.task_duration is not None else 5*(random.uniform(1, 8))

    def demo_task():
        print(f"[{args.id}] >>> EXECUTING local task (duration {duration:.2f}s) <<<")
        time.sleep(duration)
        print(f"[{args.id}] <<< local task finished >>>")

    node = ChainSyncNode(args.id, args.listen_port, succ_addr, demo_task)
    node.start()

    # Become ready at a random time to demonstrate correctness even with skewed readiness
    ready_delay = random.uniform(0, 12)
    print(f"[{args.id}] will become locally ready in {ready_delay:.2f}s")
    time.sleep(ready_delay)
    node.become_locally_ready()

    # Keep process alive until exit wave completes
    try:
        while not node.exit_flag:
            time.sleep(1)
        print("Exiting gracefully...")
        sys.exit(0)
    except KeyboardInterrupt:
        print(f"\n[{args.id}] interrupted by user")
        sys.exit(0)
