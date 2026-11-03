#!/usr/bin/env python3
"""Reproduce Solace 10.26.0.9163 ignoring maxRedeliveryCount for rejected AMQP messages.

The problem
-----------
A queue with redeliveryEnabled=true, maxRedeliveryCount=N and a deadMsgQueue
should deliver a message that a consumer settles as AMQP `rejected` 1 + N
times (the first delivery plus N redeliveries) and then move it to the DMQ.

  - 10.26.0.9054 does this.
  - 10.26.0.9163 (tagged latest/lts/10.26.0 since 2026-10-27) moves the message
    to the DMQ after the first reject. maxRedeliveryCount is ignored.

Only the broker image differs between runs: same client, same queue
configuration, same settlement outcome.

What the script does
--------------------
For each image below (working build, failing build, then whatever `latest`
points to today):
  1. pulls the image and starts a fresh broker with `docker run`
  2. creates a DMQ and a queue with maxRedeliveryCount=5 via SEMP v2
  3. opens two AMQP consumers: one on the queue that rejects every delivery,
     and one on the DMQ that accepts whatever arrives there
  4. publishes one persistent message to the topic the queue subscribes to
  5. logs every delivery on both consumers until both are idle for 10s
  6. removes the broker

It then prints a summary per image: deliveries on the queue, messages left on
the queue, messages received from the DMQ, and OK when deliveries equal
1 + maxRedeliveryCount.

Exit code is 0 when the regression reproduces (9054 OK, 9163 FAIL).

Notes
-----
  - The broker runs with OPENSSL_armcap=0. Without it, on Apple Silicon (M4) the
    SEMP v2 server in 10.26.0.9163 crashes with SIGILL at startup, because
    OpenSSL's CPU-feature detection runs an SVE instruction the Docker VM traps.
  - Host ports 18088 (SEMP) and 15672 (AMQP) are used. The container is called
    solace-redelivery-repro.
  - On first run the script creates ./.venv and installs python-qpid-proton.

Usage:
  ./repro.py
"""
import base64
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
VENV = HERE / ".venv"


def ensure_proton():
    """Re-exec inside a local .venv with python-qpid-proton installed, if needed."""
    try:
        import proton  # noqa: F401
        return
    except ImportError:
        pass
    if os.environ.get("REPRO_BOOTSTRAPPED"):
        sys.exit("python-qpid-proton is still missing after bootstrapping .venv")
    py = VENV / "bin" / "python"
    if not py.exists():
        print("Creating .venv and installing python-qpid-proton ...")
        subprocess.run([sys.executable, "-m", "venv", str(VENV)], check=True)
        subprocess.run([str(py), "-m", "pip", "install", "-q", "python-qpid-proton"], check=True)
    os.execve(str(py), [str(py), __file__, *sys.argv[1:]], {**os.environ, "REPRO_BOOTSTRAPPED": "1"})


ensure_proton()

from proton import Message, Timeout as ReceiveTimeout  # noqa: E402
from proton.utils import BlockingConnection  # noqa: E402

WORKING_IMAGE = "solace/solace-pubsub-standard:10.26.0.9054"
FAILING_IMAGE = "solace/solace-pubsub-standard:10.26.0.9163"
LATEST_IMAGE = "solace/solace-pubsub-standard:latest"
MAX_REDELIVERY = 5
EXPECTED_DELIVERIES = 1 + MAX_REDELIVERY
IDLE_SECONDS = 10

CONTAINER = "solace-redelivery-repro"
SEMP_PORT = 18088
AMQP_PORT = 15672
VPN = "default"
QUEUE = "repro/created"
DMQ = "repro/DMQ/created"
TOPIC = "repro/created"


def remove_broker():
    subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True)


def start_broker(image):
    remove_broker()
    # Pull so a cached `latest` is refreshed to whatever the tag points to today
    subprocess.run(["docker", "pull", "-q", image], check=True, stdout=subprocess.DEVNULL)
    subprocess.run([
        "docker", "run", "-d", "--name", CONTAINER,
        "-p", f"{SEMP_PORT}:8080", "-p", f"{AMQP_PORT}:5672",
        "--shm-size=1g", "--ulimit", "nofile=1048576:1048576",
        "-e", "solace_accept_license=1",
        "-e", "username_admin_globalaccesslevel=admin",
        "-e", "username_admin_password=admin",
        "-e", "system_scaling_maxconnectioncount=100",
        # Apple M4 + Docker: OpenSSL probes SVE and the SEMP server dies with SIGILL without this
        "-e", "OPENSSL_armcap=0",
        image,
    ], check=True, stdout=subprocess.DEVNULL)


def solos_version():
    out = subprocess.run(
        ["docker", "exec", CONTAINER, "grep", "-m1", "-o", "SolOS [0-9.]*", "/usr/sw/jail/logs/system.log"],
        capture_output=True, text=True,
    )
    return out.stdout.strip() or "unknown"


class Semp:
    def __init__(self, port):
        self.base = f"http://localhost:{port}/SEMP/v2"
        self.auth = "Basic " + base64.b64encode(b"admin:admin").decode()

    def call(self, method, path, body=None):
        req = urllib.request.Request(
            self.base + path,
            method=method,
            data=json.dumps(body).encode() if body is not None else None,
            headers={"Authorization": self.auth, "Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status, json.load(resp)
        except urllib.error.HTTPError as e:
            return e.code, json.load(e)

    def queues_path(self, kind, name=""):
        suffix = "/" + urllib.parse.quote(name, safe="") if name else ""
        return f"/{kind}/msgVpns/{VPN}/queues{suffix}"

    def post(self, path, body):
        status, resp = self.call("POST", path, body)
        if status != 200:
            raise RuntimeError(f"POST {path} failed: {resp.get('meta', {}).get('error')}")

    def msg_count(self, queue):
        _, resp = self.call("GET", self.queues_path("monitor", queue) + "/msgs")
        return len(resp.get("data", []))

    def wait_ready(self, timeout):
        """Wait until SEMP answers and the message spool accepts queue creation."""
        deadline = time.time() + timeout
        probe = {"queueName": "__spool_probe__", "accessType": "exclusive"}
        while time.time() < deadline:
            try:
                status, _ = self.call("POST", self.queues_path("config"), probe)
                if status == 200:
                    self.call("DELETE", self.queues_path("config", "__spool_probe__"))
                    return
            except (urllib.error.URLError, ConnectionError, json.JSONDecodeError):
                pass
            time.sleep(3)
        raise TimeoutError(f"Solace SEMP/spool not ready after {timeout}s (check `docker logs {CONTAINER}`)")

    def create_queues(self):
        common = {"accessType": "exclusive", "permission": "consume", "ingressEnabled": True, "egressEnabled": True}
        self.post(self.queues_path("config"), {"queueName": DMQ, **common})
        self.post(self.queues_path("config"), {"queueName": QUEUE, **common, "deadMsgQueue": DMQ,
                                               "redeliveryEnabled": True, "maxRedeliveryCount": MAX_REDELIVERY})
        self.post(self.queues_path("config", QUEUE) + "/subscriptions", {"subscriptionTopic": TOPIC})


def receive_now(receiver):
    try:
        return receiver.receive(timeout=0.2)
    except ReceiveTimeout:
        return None


def publish_and_consume():
    """Reject everything on the queue and accept everything on the DMQ until both stay idle."""
    conn = BlockingConnection(f"amqp://localhost:{AMQP_PORT}", user="default", password="default")
    try:
        queue_rx = conn.create_receiver(QUEUE, credit=1)
        dmq_rx = conn.create_receiver(DMQ, credit=1)

        conn.create_sender(TOPIC).send(Message(id="repro-1", body="redelivery repro", durable=True))
        report_published()

        deliveries = dmq_received = 0
        last_activity = time.time()
        while time.time() - last_activity < IDLE_SECONDS:
            if msg := receive_now(queue_rx):
                deliveries += 1
                queue_rx.reject()
                report_rejected(deliveries, msg)
                last_activity = time.time()
            if msg := receive_now(dmq_rx):
                dmq_received += 1
                dmq_rx.accept()
                report_dmq_received(msg)
                last_activity = time.time()
        return deliveries, dmq_received
    finally:
        conn.close()


def run(image):
    report_start(image)
    start_broker(image)
    try:
        semp = Semp(SEMP_PORT)
        semp.wait_ready(timeout=300)
        version = solos_version()
        semp.create_queues()
        report_broker_ready(version)

        deliveries, dmq_received = publish_and_consume()
        return {"image": image, "version": version, "deliveries": deliveries,
                "ok": deliveries == EXPECTED_DELIVERIES,
                "queue": semp.msg_count(QUEUE), "dmq": dmq_received}
    finally:
        remove_broker()


# --- reporting -------------------------------------------------------------

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def report_start(image):
    log(f"=== {image} ===")


def report_broker_ready(version):
    log(f"Broker ready ({version}); created {QUEUE} with maxRedeliveryCount={MAX_REDELIVERY}, DMQ={DMQ}")


def report_published():
    log(f"Published 1 persistent message to {TOPIC}; rejecting every delivery on {QUEUE}, accepting on {DMQ}")


def report_rejected(n, msg):
    log(f"  [queue] received delivery #{n} id={msg.id} delivery_count={msg.delivery_count} -> rejected")


def report_dmq_received(msg):
    log(f"  [dmq]   received id={msg.id} -> accepted")


def print_summary(results):
    print()
    print(f"maxRedeliveryCount={MAX_REDELIVERY}, every delivery rejected "
          f"-> expected {EXPECTED_DELIVERIES} deliveries, then DMQ")
    print()
    print(f"{'image':46} {'version':20} {'deliveries':>10} {'left in queue':>13} {'dmq':>4}  result")
    for r in results:
        verdict = "OK" if r["ok"] else "FAIL, maxRedeliveryCount not respected"
        print(f"{r['image']:46} {r['version']:20} {r['deliveries']:>10} {r['queue']:>13} {r['dmq']:>4}  {verdict}")


def main():
    working, failing, latest = run(WORKING_IMAGE), run(FAILING_IMAGE), run(LATEST_IMAGE)
    print_summary([working, failing, latest])
    return 0 if working["ok"] and not failing["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
