import ssl
import sys
import time
import socket
import random
import argparse
import threading


class ChainSyncNode:
    def __init__(
        self,
        node_id: str,
        listen_port: int | None,
        succ_addr: tuple[str, int] | None,
        task_func,
        no_mtls,
        no_localhost,
        tls_key,
        tls_cert,
        tls_cert_trust,
        no_exit,
    ):
        self.node_id = node_id
        self.listen_port = listen_port
        self.succ_addr = succ_addr
        self.task_func = task_func
        self.no_mtls = no_mtls
        self.hostname = socket.gethostname() if no_localhost else "localhost"
        self.tls_key = tls_key
        self.tls_cert = tls_cert
        self.tls_cert_trust = tls_cert_trust

        self.server_context = None
        self.client_context = None

        self.is_head = listen_port is None
        self.is_tail = succ_addr is None

        self.pred_socket: socket.socket | None = None
        self.succ_socket: socket.socket | None = None
        self.server_socket: socket.socket | None = None

        self.local_ready = {}  # False
        self.state = {}  # SYNC, READY, WATCH, START, COMPLETE
        self.pred_ready = {}  # self.is_head
        self.pred_complete = {}  # self.is_head
        self.has_ready_from_pred = {}  # False
        self.has_complete_from_succ = {}  # False
        self.has_start_from_succ = {}  # False

        self.no_exit = no_exit
        self.exit_flag = False

    def message_pred(self, msg: str, round_id: str):
        if self.pred_socket:
            try:
                print(
                    f"[{self.node_id}] sending message {msg}:{round_id} to predecessor"
                )
                self.pred_socket.sendall((msg + ":" + round_id + "\n").encode("utf-8"))
            except Exception as ex:
                print(f"Exception messaging predecessor: {ex}")
        return None

    def message_succ(self, msg: str, round_id: str):
        if self.succ_socket:
            try:
                print(f"[{self.node_id}] sending message {msg}:{round_id} to successor")
                self.succ_socket.sendall((msg + ":" + round_id + "\n").encode("utf-8"))
            except Exception as ex:
                print(f"Exception messaging successor: {ex}")
        return None

    def execute_task(self, round_id):
        try:
            self.task_func(round_id)
        finally:
            self.task_completed(round_id)
        return None

    def task_completed(self, round_id):
        self.transition_state(round_id)
        if not self.is_tail:
            self.message_succ("COMPLETE", round_id)
        else:
            self.message_pred("COMPLETE", round_id)
            self.exit()
        return None

    def become_locally_ready(self, round_id):
        self.local_ready.update({round_id: True})
        print(f"[{self.node_id}] ({round_id}) locally ready")
        self.transition_state(round_id)
        return None

    def exit(self):
        if self.no_exit:
            pass
        else:
            print(f"[{self.node_id}] Setting exit flag...")
            self.exit_flag = True
        return None

    def init_state(self, round_id):
        if round_id in self.state:
            pass
        else:
            self.state.update({round_id: "SYNC"})
        if round_id in self.pred_ready:
            pass
        else:
            self.pred_ready.update({round_id: self.is_head})
        if round_id in self.pred_complete:
            pass
        else:
            self.pred_complete.update({round_id: self.is_head})
        if round_id in self.has_ready_from_pred:
            pass
        else:
            self.has_ready_from_pred.update({round_id: False})
        if round_id in self.has_complete_from_succ:
            pass
        else:
            self.has_complete_from_succ.update({round_id: False})
        if round_id in self.has_start_from_succ:
            pass
        else:
            self.has_start_from_succ.update({round_id: False})
        return None

    def check_local_ready(self, round_id):
        result = False
        if round_id in self.local_ready:
            result = self.local_ready[round_id]
        return result

    def transition_state(self, round_id):
        self.init_state(round_id)
        start_state = self.state[round_id]
        if (
            self.state[round_id] == "SYNC"
            and self.pred_ready[round_id]
            and self.check_local_ready(round_id)
        ):
            self.state[round_id] = "READY"
            print(
                f"[{self.node_id}] (round_id:{round_id}) State transition: {start_state} -> {self.state[round_id]}"
            )
            if self.is_tail:
                self.has_start_from_succ[round_id] = (
                    True  # Tail initiates the backward START wave
                )
                self.transition_state(round_id)
            else:
                self.message_succ("READY", round_id)
        elif (
            self.state[round_id] == "READY"
            and self.is_head
            and self.has_start_from_succ[round_id]
        ):
            self.state[round_id] = "START"
            print(
                f"[{self.node_id}] (round_id:{round_id}) State transition: {start_state} -> {self.state[round_id]}"
            )
            self.execute_task(round_id)
        elif (
            self.state[round_id] == "READY"
            and not self.is_head
            and self.has_start_from_succ[round_id]
        ):
            self.state[round_id] = "WATCH"
            print(
                f"[{self.node_id}] (round_id:{round_id}) State transition: {start_state} -> {self.state[round_id]}"
            )
            self.message_pred("START", round_id)
        elif self.state[round_id] == "WATCH" and self.pred_complete[round_id]:
            self.state[round_id] = "START"
            print(
                f"[{self.node_id}] (round_id:{round_id}) State transition: {start_state} -> {self.state[round_id]}"
            )
            self.execute_task(round_id)
        elif self.state[round_id] == "START":
            self.state[round_id] = "COMPLETE"
            print(
                f"[{self.node_id}] (round_id:{round_id}) State transition: {start_state} -> {self.state[round_id]}"
            )
        else:  # do nothing
            print(
                f"[{self.node_id}] (round_id:{round_id}) State transition: {start_state} -> {self.state[round_id]}"
            )
        return None

    def handle_message(self, cmd: str, direction: str, round_id: str):
        print(f"[{self.node_id}] received {cmd}:{round_id} from {direction}")
        self.init_state(round_id)
        if cmd == "READY":
            if direction == "pred":
                self.pred_ready[round_id] = True
                self.transition_state(round_id)
        elif cmd == "START":
            if direction == "succ":
                self.has_start_from_succ[round_id] = True
                self.transition_state(round_id)
        elif cmd == "COMPLETE":
            if direction == "pred":
                self.pred_complete[round_id] = True
                self.transition_state(round_id)
            elif direction == "succ":
                self.message_pred("COMPLETE", round_id)
                self.exit()
        return None

    def _reader(self, sock: socket.socket, direction: str):
        try:
            f = sock.makefile("r", encoding="utf-8")
            while True:
                line = f.readline()
                if not line:
                    break
                cmd, round_id = line.strip().split(":")
                if cmd:
                    self.handle_message(cmd, direction, round_id)
        except Exception as e:
            print(f"[{self.node_id}] reader error on {direction}: {e}")
        finally:
            sock.close()
        return None

    def _accept_pred(self):
        try:
            conn, addr = self.server_socket.accept()
            print(f"[{self.node_id}] predecessor connected from {addr}")
            if self.no_mtls:
                self.pred_socket = conn
                threading.Thread(
                    target=self._reader, args=(conn, "pred"), daemon=True
                ).start()
            else:
                tls_sock = self.server_context.wrap_socket(conn, server_side=True)
                self.pred_socket = tls_sock
                threading.Thread(
                    target=self._reader, args=(tls_sock, "pred"), daemon=True
                ).start()

        except Exception as e:
            print(f"[{self.node_id}] accept error: {e}")
        return None

    def start(self):
        print(
            f"[{self.node_id}] starting (head={'yes' if self.is_head else 'no'}, tail={'yes' if self.is_tail else 'no'})"
        )
        if not self.is_head:
            if self.no_mtls:
                pass
            else:
                self.server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                self.server_context.load_cert_chain(
                    certfile=self.tls_cert, keyfile=self.tls_key
                )
                self.server_context.load_verify_locations(self.tls_cert_trust)
                self.server_context.verify_mode = ssl.CERT_REQUIRED
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind(("", self.listen_port))
            self.server_socket.listen(1)
            print(
                f"[{self.node_id}] listening on port {self.listen_port} for predecessor"
            )
            threading.Thread(target=self._accept_pred, daemon=True).start()

        if not self.is_tail:
            self.succ_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if self.no_mtls:
                pass
            else:
                self.client_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                self.client_context.load_cert_chain(
                    certfile=self.tls_cert, keyfile=self.tls_key
                )
                self.client_context.load_verify_locations(self.tls_cert_trust)
                self.client_context.verify_mode = ssl.CERT_REQUIRED
                sock = self.succ_socket
                self.succ_socket = self.client_context.wrap_socket(
                    sock, server_hostname=self.hostname
                )
            print(f"[{self.node_id}] connecting to successor {self.succ_addr} ...")
            while True:
                try:
                    self.succ_socket.connect(self.succ_addr)
                    break
                except ConnectionRefusedError:
                    time.sleep(0.5)
            print(f"[{self.node_id}] connected to successor")
            threading.Thread(
                target=self._reader, args=(self.succ_socket, "succ"), daemon=True
            ).start()
        while (not self.is_head and self.pred_socket is None) or (
            not self.is_tail and self.succ_socket is None
        ):
            print(f"[{self.node_id}] waiting for connection...")
            time.sleep(0.5)
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ChainSync reference implementation")
    parser.add_argument("--id", required=True, help="Node identifier (A, B, C, ...)")
    parser.add_argument(
        "--listen-port",
        type=int,
        help="Port to listen on for predecessor (omit for head).",
    )
    parser.add_argument(
        "--succ-host", default="localhost", help="Successor host (default: localhost)."
    )
    parser.add_argument("--succ-port", type=int, help="Successor port (omit for tail).")
    parser.add_argument(
        "--task-duration",
        type=float,
        default=None,
        help="Task duration in seconds (default: random 1-8s).",
    )
    parser.add_argument("--no-mtls", action="store_true", help="Disable mutual TLS.")
    parser.add_argument(
        "--no-localhost",
        action="store_true",
        help="Use the hostname and not 'localhost'.",
    )
    parser.add_argument(
        "--tls-key", type=str, default=None, help="The node's private key."
    )
    parser.add_argument(
        "--tls-cert", type=str, default=None, help="The node's certificate."
    )
    parser.add_argument(
        "--tls-cert-trust", type=str, default=None, help="The root trust certificate."
    )
    parser.add_argument(
        "--n-rounds", type=int, default=1, help="Number of concurrent rounds. Note that all nodes should have the same number of rounds. "
    )
    parser.add_argument(
        "--no-exit",
        action="store_true",
        help="Do not exit after task completion.",
    )

    args = parser.parse_args()

    succ_addr = (args.succ_host, args.succ_port) if args.succ_port else None

    duration = (
        args.task_duration
        if args.task_duration is not None
        else (random.uniform(1, 20))
    )

    def demo_task(round_id):
        epsilon = random.uniform(1, 10)
        task_duration = duration + epsilon
        print(
            f"[{args.id}] (round_id:{round_id}) >>> EXECUTING local task (duration {task_duration:.2f}s) <<<"
        )
        time.sleep(task_duration)
        print(f"[{args.id}] (round_id:{round_id}) <<< local task finished >>>")

    node = ChainSyncNode(
        args.id,
        args.listen_port,
        succ_addr,
        demo_task,
        args.no_mtls,
        args.no_localhost,
        args.tls_key,
        args.tls_cert,
        args.tls_cert_trust,
        args.no_exit,
    )
    node.start()
    n_rounds = args.n_rounds
    round_ids = []
    if n_rounds > 1:
        round_ids = [str(i) for i in range(n_rounds)]
    else:
        round_ids = [""]

    for round_id in round_ids:
        # Become ready at a random time to demonstrate correctness even with skewed readiness
        ready_delay = random.uniform(0, 12)
        print(
            f"[{args.id}] ({round_id}) will become locally ready in {ready_delay:.2f}s"
        )
        time.sleep(ready_delay)
        node.become_locally_ready(round_id)

    # Keep process alive until exit wave completes
    try:
        while not node.exit_flag:
            time.sleep(1)
        print("Exiting gracefully...")
        sys.exit(0)
    except KeyboardInterrupt:
        print(f"\n[{args.id}] interrupted by user")
        sys.exit(0)
