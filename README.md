# pylsl-server

A Lab Streaming Layer (LSL) listener for event markers advertised by another machine on the local network.

LSL is a publish/subscribe overlay: the remote machine creates an **outlet**, this machine **resolves** that stream over multicast, then pulls timestamped samples over TCP. Discovery does not need a host or port.

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

`pylsl` needs the `liblsl` shared library. On macOS that is often missing after `pip install`. Install it, then point pylsl at it if needed:

```bash
# conda
conda install -c conda-forge liblsl

# or download a release from https://github.com/sccn/liblsl/releases
export PYLSL_LIB=/usr/local/lib/liblsl.dylib
```

## Listen on this machine

```bash
python -m lsl_server listen
```

The listener stays up and connects to every LAN stream whose LSL type is `Markers`. Clock-sync post-processing remaps remote timestamps onto this machine's LSL clock. An `end` marker closes that inlet so LSL does not try to reconnect after the remote session finishes. A later run with a new stream uid is still accepted.

Useful filters when several machines are on the same subnet:

```bash
python -m lsl_server listen --hostname stim-pc
python -m lsl_server listen --name LabEvents
python -m lsl_server listen --type Markers --log events.jsonl -v
```

`--type ''` matches every content type, not just markers.

## Send events from the other machine

Copy this project (or just run the same module) on the machine that produces events:

```bash
python -m lsl_server send --name LabEvents
```

Then type a marker and press Enter. For a one-shot or a timed demo:

```bash
python -m lsl_server send --event trial_start
python -m lsl_server send --interval 1.0
```

Any LSL-capable app can be the source. The listener only requires a stream with type `Markers` (or whatever you pass to `--type`).

## See what is on the network

```bash
python -m lsl_server discover --wait 3
```

## LAN checklist

Both machines must be on the same subnet (or otherwise able to exchange LSL multicast). If discovery fails:

1. Confirm the sender is running *before* or while the listener is up. `listen` uses a continuous resolver, so late-appearing streams are still picked up.
2. Allow LSL through the firewall on both machines: UDP 16571 (discovery) and TCP/UDP 16572–16604 (data).
3. Avoid guest/public Wi-Fi networks that isolate clients from each other.
4. Match `--hostname` to the name printed by `discover`, not the IP address.

## Use from Python

```python
from lsl_server.listener import EventListener, ReceivedEvent
from lsl_server.streams import StreamFilters

def handle(event: ReceivedEvent) -> None:
    print(event.hostname, event.sample)

EventListener(
    filters=StreamFilters(stream_type="Markers", hostname="stim-pc"),
).run()
```
