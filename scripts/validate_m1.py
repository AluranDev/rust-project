"""Run the built server and two independent synthetic clients on loopback.

Run `cargo build --workspace --offline` first, then `python scripts/validate_m1.py`.
"""

from contextlib import ExitStack
from pathlib import Path
import os
import secrets
import socket
import subprocess
import time


def main():
    root = Path(__file__).resolve().parents[1]
    output = root / "target" / "m1-validation"
    output.mkdir(parents=True, exist_ok=True)
    suffix = ".exe" if os.name == "nt" else ""
    flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    with socket.socket() as probe:
        if probe.connect_ex(("127.0.0.1", 3000)) == 0:
            raise RuntimeError("Port 3000 is occupied; this script requires its own server")
    env = dict(os.environ, VOICE_SIGNALING_URL="ws://127.0.0.1:3000/ws",
               VOICE_SIGNALING_TOKEN=secrets.token_urlsafe(32))
    processes = []
    with ExitStack() as files:
        def start(binary, arguments, label):
            log = files.enter_context((output / f"{label}.log").open("w", encoding="utf-8"))
            process = subprocess.Popen(
                [str(root / "target" / "debug" / f"{binary}{suffix}"), *arguments],
                cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT,
                creationflags=flags,
            )
            processes.append(process)
            return process

        try:
            server = start("signaling-server", [], "server")
            deadline = time.monotonic() + 10
            while True:
                if server.poll() is not None:
                    raise RuntimeError("Server exited before listening; inspect server.log")
                with socket.socket() as probe:
                    if probe.connect_ex(("127.0.0.1", 3000)) == 0:
                        break
                if time.monotonic() >= deadline:
                    raise TimeoutError("Server did not start")
                time.sleep(0.05)
            clients = [(peer, start("voice-client", ["synthetic", peer, "m1"], peer))
                       for peer in ("alice", "bob")]
            for peer, client in clients:
                code = client.wait(timeout=30)
                if code != 0:
                    raise RuntimeError(f"{peer} exited with {code}; inspect {peer}.log")
        finally:
            for process in processes:
                if process.poll() is None:
                    process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
    for peer, _ in clients:
        log = (output / f"{peer}.log").read_text(encoding="utf-8")
        frames = log.count("Decoded 1920 samples from")
        assert frames == 50, f"{peer}: expected 50 decoded frames, got {frames}"
        assert "Receiver stopped naturally:" in log, f"{peer}: receiver did not finish"
        print(f"{peer}: exit 0, {frames} decoded frames, receiver stopped naturally")
    print(f"Logs: {output}")


if __name__ == "__main__":
    main()
