"""Reproducer: DEL + SET traffic during a forkless BGSAVE trips bgiteration.c 'wasAdded'.

Usage: python3 forkless_del_during_save_repro.py <valkey-server> <trials> fresh|restart del|nodel

Each trial starts a server with forkless saves, loads 3200 keys, optionally
SIGKILLs and restarts it from AOF, starts BGSAVE (stretched with
rdb-key-save-delay 500us), and runs SET/DEL traffic on 200 keys for 4s.
See docs/findings/forkless-early-iterate-assertion.md.
"""

import os
import random
import shutil
import signal
import subprocess
import sys
import tempfile
import time

import valkey

BIN = sys.argv[1]
TRIALS = int(sys.argv[2])
RESTART = sys.argv[3] == "restart"
DEL = sys.argv[4] == "del"
PORT = 8350


def start(d):
    p = subprocess.Popen(
        [
            BIN,
            "--port",
            str(PORT),
            "--dir",
            d,
            "--logfile",
            os.path.join(d, "log"),
            "--save",
            "",
            "--appendonly",
            "yes",
            "--forkless-infrastructure-enabled",
            "yes",
            "--bgsave-default-method",
            "forkless",
            "--rdb-key-save-delay",
            "500",
        ]
    )
    for _ in range(200):
        try:
            c = valkey.Valkey(port=PORT, decode_responses=True)
            c.ping()
            return p, c
        except Exception:
            time.sleep(0.05)
    raise SystemExit("server did not start")


crashes = 0
for trial in range(TRIALS):
    d = tempfile.mkdtemp(prefix="fr-")
    rng = random.Random(trial)
    p, c = start(d)
    pipe = c.pipeline(transaction=False)
    for i in range(3000):
        pipe.set(f"s:{i}", "x" * 64)
    for i in range(200):
        pipe.set(f"m:{i}", "y" * 64)
    pipe.execute()
    if RESTART:
        os.kill(p.pid, signal.SIGKILL)
        p.wait()
        p, c = start(d)
    c.execute_command("BGSAVE")
    deadline = time.time() + 4
    crashed = False
    while time.time() < deadline:
        key = f"m:{rng.randrange(200)}"
        try:
            if DEL and rng.random() < 0.15:
                c.delete(key)
            else:
                c.set(key, "z" * rng.randrange(1, 128))
        except Exception:
            crashed = True
            break
        time.sleep(0.004)
    if p.poll() is not None or crashed:
        time.sleep(0.2)
        with open(os.path.join(d, "log")) as handle:
            log = handle.read()
        if "wasAdded" in log:
            crashes += 1
            print(f"trial {trial}: ASSERTION wasAdded (log {d}/log)")
            shutil.copy(os.path.join(d, "log"), f"crash-{trial}.log")
    if p.poll() is None:
        p.terminate()
        p.wait()
    shutil.rmtree(d, ignore_errors=True)
print(f"{'restart' if RESTART else 'fresh'}: {crashes}/{TRIALS} trials hit the assertion")
