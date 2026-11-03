# Solace 10.26.0.9163: `maxRedeliveryCount` ignored for rejected AMQP messages

In `solace/solace-pubsub-standard:10.26.0.9163` (currently tagged `latest`, `lts` and `10.26.0`), a persistent message that an AMQP consumer settles as **rejected** goes straight to the queue's DMQ after the first delivery. The queue's `maxRedeliveryCount` is ignored.

In the previous build, `10.26.0.9054`, the same message is redelivered `maxRedeliveryCount` times and then moved to the DMQ.

Nothing differs between the runs except the broker image: same client, same queue configuration, same settlement outcome.

As of 2026-11-03, `latest` still resolves to 10.26.0.9163 and shows the same problem.

## Expected behaviour

A queue with `redeliveryEnabled=true`, `maxRedeliveryCount=N` and a `deadMsgQueue` should deliver a rejected message `1 + N` times (the first delivery plus N redeliveries) before moving it to the DMQ. This is what `10.26.0.9054` does.

## Actual behaviour on 10.26.0.9163

The message is delivered once and moved to the DMQ right after the first reject. The `maxRedeliveryCount` value makes no difference. We saw the same behaviour with values of 1, 2, 4 and 5.

## Reproduction

`repro.py` runs the scenario against three images in turn: `10.26.0.9054` (working), `10.26.0.9163` (failing), and `latest` (whatever the tag points to on the day you run it). For each image it:

1. Pulls the image and starts a fresh broker with `docker run`.
2. Uses SEMP v2 to create a DMQ and a queue with `redeliveryEnabled=true`, `maxRedeliveryCount=5` and `deadMsgQueue` set to that DMQ, then subscribes the queue to a topic.
3. Publishes one persistent (`durable=true`) message to the topic over AMQP 1.0.
4. Consumes the queue and settles every delivery as `rejected`, while a second consumer on the DMQ accepts whatever arrives there.
5. Prints every delivery on both consumers until both have been idle for 10 seconds, then removes the broker.

At the end it prints a summary table with the resolved SolOS version of each image.

### Requirements

- Docker
- Python 3. On first run the script creates a local `.venv` and installs `python-qpid-proton` into it.

### Run

```
./repro.py
```

The script uses host ports `18088` (SEMP) and `15672` (AMQP). It exits with 0 when the difference reproduces, meaning 9054 respects the redelivery count and 9163 doesn't. The `latest` run is informational and doesn't affect the exit code. A full run takes about two minutes.

### Output

```
=== solace/solace-pubsub-standard:10.26.0.9054 ===
Broker ready (SolOS 10.26.0.9054); created repro/created with maxRedeliveryCount=5, DMQ=repro/DMQ/created
Published 1 persistent message to repro/created; rejecting every delivery on repro/created, accepting on repro/DMQ/created
  [queue] received delivery #1 id=repro-1 delivery_count=0 -> rejected
  [queue] received delivery #2 id=repro-1 delivery_count=1 -> rejected
  [queue] received delivery #3 id=repro-1 delivery_count=1 -> rejected
  [queue] received delivery #4 id=repro-1 delivery_count=1 -> rejected
  [queue] received delivery #5 id=repro-1 delivery_count=1 -> rejected
  [queue] received delivery #6 id=repro-1 delivery_count=1 -> rejected
  [dmq]   received id=repro-1 -> accepted
=== solace/solace-pubsub-standard:10.26.0.9163 ===
Broker ready (SolOS 10.26.0.9163); created repro/created with maxRedeliveryCount=5, DMQ=repro/DMQ/created
Published 1 persistent message to repro/created; rejecting every delivery on repro/created, accepting on repro/DMQ/created
  [queue] received delivery #1 id=repro-1 delivery_count=0 -> rejected
  [dmq]   received id=repro-1 -> accepted
=== solace/solace-pubsub-standard:latest ===
Broker ready (SolOS 10.26.0.9163); created repro/created with maxRedeliveryCount=5, DMQ=repro/DMQ/created
Published 1 persistent message to repro/created; rejecting every delivery on repro/created, accepting on repro/DMQ/created
  [queue] received delivery #1 id=repro-1 delivery_count=0 -> rejected
  [dmq]   received id=repro-1 -> accepted

maxRedeliveryCount=5, every delivery rejected -> expected 6 deliveries, then DMQ

image                                          version              deliveries left in queue  dmq  result
solace/solace-pubsub-standard:10.26.0.9054     SolOS 10.26.0.9054            6             0    1  OK
solace/solace-pubsub-standard:10.26.0.9163     SolOS 10.26.0.9163            1             0    1  FAIL, maxRedeliveryCount not respected
solace/solace-pubsub-standard:latest           SolOS 10.26.0.9163            1             0    1  FAIL, maxRedeliveryCount not respected
```

(Output from 2026-11-03, timestamps removed.)

### How to read the output

- **10.26.0.9054:** the message is rejected 6 times (1 delivery + 5 redeliveries) and then arrives on the DMQ consumer. The redelivery count is respected.
- **10.26.0.9163:** the message is rejected once and arrives on the DMQ consumer immediately. No redeliveries happen.
- **latest:** shows which build the tag currently points to. On 2026-11-03 that is still 10.26.0.9163, with the same failure. Once `latest` moves to a new build, this row shows whether it has been fixed.
- **On every build:** the message ends up in the DMQ and nothing is left on the queue, so no message is lost. Only the redelivery step is skipped.

A separate observation: on 9054 the AMQP `delivery-count` header stays at `1` on every redelivery instead of counting up. It doesn't affect the result above.

### Questions

- Is this change in how the `rejected` outcome is handled intentional? The AMQP 1.0 spec describes `rejected` as "message invalid" and `modified` with `delivery-failed` as "processing failed". If the change is deliberate, it is still a behaviour change within the same 10.26.0 line, and it changes what happens to existing consumers that reject failed messages to get them retried.
- Is there a broker or queue setting that restores the previous behaviour?

## Second issue: SEMP v2 crashes with SIGILL on Apple Silicon (M4)

Also on `10.26.0.9163`, running in Docker Desktop on an Apple M4 Pro, the broker starts (the message spool becomes active and the VPN comes up), but the SEMP v2 server never starts. Requests to `:8080/SEMP/v2/*` get no response, and the debug log shows the SEMP process restarting about once a second:

```
ERROR Child process died, command: 'python3 /usr/sw/loads/soltr_10.26.0.9163/scripts/sempv2/main.py',
      status: program terminated due to non-caught signal 'Illegal instruction'
```

The crash happens while Python's `cryptography` package (50.0.2, which bundles its own OpenSSL) loads. A one-line import inside the container reproduces it:

```
docker exec <container> python3 -c "import cryptography.hazmat.bindings._rust"   # exits 132 (SIGILL)
```

Under gdb:

```
Program received signal SIGILL, Illegal instruction.
_armv8_sve_get_vl_bytes () from .../cryptography/hazmat/bindings/_rust.abi3.so
=> cntb x0
#1 OPENSSL_cpuid_setup ()
```

The Docker Desktop VM kernel (`6.12.5-linuxkit`) lists `sve2` in the CPU features, so OpenSSL's CPU-feature detection runs the SVE instruction `cntb`, which traps on this host.

Environment: Apple M4 Pro, macOS 15 (Darwin 24.6.0), Docker Desktop engine 27.5.1, image architecture `linux/arm64`.

**Workaround:** set `OPENSSL_armcap=0` on the container. This makes OpenSSL skip CPU-feature detection. With it set, SEMP starts normally. `repro.py` sets it for every image so that it can run on Apple Silicon:

```
docker run ... -e OPENSSL_armcap=0 solace/solace-pubsub-standard:10.26.0.9163
```
