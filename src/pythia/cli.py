"""Minimal CLI: starts the background process (daemon + web) and nothing else.

    pythia start      starts the database (Docker) and the background process
    pythia stop       stops the process
    pythia status     shows whether it is running and where
    pythia run        runs in the foreground (what `start` executes)
    pythia autostart  starts by itself at macOS login (LaunchAgent); `--off` undoes it
    pythia boot       what the LaunchAgent executes: waits for Docker Desktop and runs `start`

All configuration happens on the web.
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from . import __version__

HOME = Path(os.environ.get("PYTHIA_HOME", Path.home() / ".pythia"))
PIDFILE = HOME / "pythia.pid"
LOGFILE = HOME / "pythia.log"
REPO = Path(__file__).resolve().parents[2]
CONTAINER = "pythia-db"


def _compose_file() -> Path:
    """docker-compose.yml shipped inside the package (pip install) or at the repository root (source checkout)."""
    for candidate in (Path(__file__).parent / "docker-compose.yml", REPO / "docker-compose.yml"):
        if candidate.exists():
            return candidate
    raise SystemExit("docker-compose.yml not found; set PYTHIA_DATABASE_URL to use your own Postgres")


def _pid() -> int | None:
    try:
        pid = int(PIDFILE.read_text().strip())
        os.kill(pid, 0)
        return pid
    except (FileNotFoundError, ValueError, ProcessLookupError, PermissionError):
        return None


def ensure_database() -> None:
    if os.environ.get("PYTHIA_DATABASE_URL"):
        return
    state = subprocess.run(["docker", "inspect", "-f", "{{.State.Running}}", CONTAINER],
                           capture_output=True, text=True)
    if state.returncode == 0:
        if state.stdout.strip() != "true":
            subprocess.run(["docker", "start", CONTAINER], check=True, capture_output=True)
    else:
        subprocess.run(["docker", "compose", "-p", "pythia", "-f", str(_compose_file()), "up", "-d"], check=True)
    from .db import database_url
    import psycopg
    for _ in range(30):
        try:
            psycopg.connect(database_url(), connect_timeout=2).close()
            return
        except psycopg.OperationalError:
            time.sleep(1)
    raise SystemExit("Postgres did not respond in 30s")


def cmd_run(_args) -> None:
    import uvicorn

    from .daemon import Daemon
    from .db import Database
    from .services import build
    from .web.app import create_app

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ensure_database()
    db = Database()
    db.open()
    svc = build(db)
    daemon = Daemon(svc)
    daemon.start()
    port = int(svc.settings.get("web")["port"])
    try:
        uvicorn.run(create_app(svc), host="127.0.0.1", port=port, log_level="warning")
    finally:
        daemon.shutdown()
        db.close()


def cmd_start(_args) -> None:
    running = _all_pids()
    if running:
        print(f"Already running (pid {', '.join(map(str, running))}). Use `pythia stop` first.")
        return
    HOME.mkdir(parents=True, exist_ok=True)
    ensure_database()
    log = open(LOGFILE, "ab")
    proc = subprocess.Popen([sys.executable, "-m", "pythia.cli", "run"], stdout=log, stderr=log,
                            stdin=subprocess.DEVNULL, start_new_session=True)
    PIDFILE.write_text(str(proc.pid))
    time.sleep(2)
    if proc.poll() is not None:
        raise SystemExit(f"Failed to start; see {LOGFILE}")
    print(f"Pythia running (pid {proc.pid}) — http://127.0.0.1:{_port()}  · log: {LOGFILE}")


def _port() -> int:
    try:
        from .db import Database
        from .settings import Settings
        db = Database(min_size=1, max_size=1)
        db.open()
        port = int(Settings(db).get("web")["port"])
        db.close()
        return port
    except Exception:
        return 7117


def _all_pids() -> list[int]:
    """All `pythia run` processes (not just the pidfile's): an orphan daemon running old code
    would pick up jobs from the queue and fail on them."""
    out = subprocess.run(["pgrep", "-f", "pythia.cli run"], capture_output=True, text=True)
    pids = [int(p) for p in out.stdout.split() if p.strip() and int(p) != os.getpid()]
    # Python processes only: a shell whose command line mentions the text is not the daemon.
    return [p for p in pids if "python" in subprocess.run(["ps", "-o", "comm=", "-p", str(p)],
                                                          capture_output=True, text=True).stdout.lower()]


def cmd_stop(_args) -> None:
    pids = sorted(set(_all_pids()) | ({_pid()} if _pid() else set()))
    if not pids:
        print("Not running.")
        return
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    for _ in range(20):
        if not _all_pids():
            break
        time.sleep(0.5)
    for pid in _all_pids():  # did not exit in 10s: force it
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    PIDFILE.unlink(missing_ok=True)
    print("Stopped." if len(pids) == 1 else f"Stopped ({len(pids)} processes).")


def cmd_status(_args) -> None:
    pid = _pid()
    print(f"Running (pid {pid}) — http://127.0.0.1:{_port()}" if pid else "Stopped.")
    print("Starts by itself at login (LaunchAgent)." if AGENT_PLIST.exists() else "Does not start by itself at login (`pythia autostart`).")


# ---------------------------------------------------------------- start at login (macOS)

AGENT_LABEL = "local.pythia"
AGENT_PLIST = Path.home() / "Library" / "LaunchAgents" / f"{AGENT_LABEL}.plist"
AUTOSTART_LOG = HOME / "autostart.log"
DOCKER_PATHS = ["/usr/local/bin", "/opt/homebrew/bin", "/Applications/Docker.app/Contents/Resources/bin", "/usr/bin", "/bin"]


def _docker_ready() -> bool:
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=20).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def cmd_boot(_args) -> None:
    """At login: Postgres runs in Docker Desktop, so it must be up first. Opens
    Docker Desktop in the background if it is not open, waits up to 5 min and runs `start`."""
    def stamp() -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S")

    if os.environ.get("PYTHIA_DATABASE_URL") is None and not _docker_ready():
        print(f"{stamp()} Docker Desktop is not responding; opening it in the background.", flush=True)
        subprocess.run(["open", "-g", "-a", "Docker"], capture_output=True)
        for _ in range(60):
            time.sleep(5)
            if _docker_ready():
                break
        else:
            raise SystemExit(f"{stamp()} Docker Desktop did not respond in 5 min; Pythia did not start. "
                             "Open Docker Desktop and run `pythia start`.")
    print(f"{stamp()} Docker ready; starting Pythia.", flush=True)
    cmd_start(_args)


def cmd_autostart(args) -> None:
    import plistlib

    uid = os.getuid()
    if args.off:
        if AGENT_PLIST.exists():
            subprocess.run(["launchctl", "bootout", f"gui/{uid}", str(AGENT_PLIST)], capture_output=True)
            AGENT_PLIST.unlink()
        print("Pythia no longer starts by itself at login.")
        return
    HOME.mkdir(parents=True, exist_ok=True)
    AGENT_PLIST.parent.mkdir(parents=True, exist_ok=True)
    plist = {
        "Label": AGENT_LABEL,
        "ProgramArguments": [sys.executable, "-m", "pythia.cli", "boot"],
        "RunAtLoad": True,
        # `start` leaves the process running on its own; the agent ends right afterwards. Without
        # KeepAlive, so that `pythia stop` keeps really stopping it.
        "AbandonProcessGroup": True,
        "WorkingDirectory": str(HOME),
        "EnvironmentVariables": {"PATH": ":".join(DOCKER_PATHS)},
        "StandardOutPath": str(AUTOSTART_LOG),
        "StandardErrorPath": str(AUTOSTART_LOG),
    }
    if AGENT_PLIST.exists():
        subprocess.run(["launchctl", "bootout", f"gui/{uid}", str(AGENT_PLIST)], capture_output=True)
    AGENT_PLIST.write_bytes(plistlib.dumps(plist))
    r = subprocess.run(["launchctl", "bootstrap", f"gui/{uid}", str(AGENT_PLIST)], capture_output=True, text=True)
    if r.returncode != 0:
        raise SystemExit(f"launchctl rejected the agent: {r.stderr.strip()}")
    print(f"Pythia starts by itself at login (opens Docker Desktop if needed). Agent: {AGENT_PLIST} · log: {AUTOSTART_LOG}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="pythia", description="Personal knowledge oracle.")
    parser.add_argument("--version", action="version", version=f"pythia {__version__}")
    sub = parser.add_subparsers(dest="cmd", required=True)
    for name, fn in (("start", cmd_start), ("stop", cmd_stop), ("status", cmd_status), ("run", cmd_run),
                     ("boot", cmd_boot)):
        sub.add_parser(name).set_defaults(fn=fn)
    auto = sub.add_parser("autostart")
    auto.add_argument("--off", action="store_true", help="stop starting by itself at login")
    auto.set_defaults(fn=cmd_autostart)
    args = parser.parse_args(argv)
    args.fn(args)


if __name__ == "__main__":
    main()
