# 80 - Network Cabling Verification (`v1.1.0`)

[← Back to README](../../README.md)

Proves that each NIC is plugged into the switch port the design says it should
be, by comparing **live LLDP neighbours** against the **cabling documented in
NetBox**. The failure it exists to catch is the one nobody notices at install
time: a transceiver seated in the port next to the planned one. The link comes
up, the host gets carrier, every downstream test passes — and the machine is
wired into the wrong leaf, the wrong fabric, or the wrong tenant's VLAN.

It runs before the GPU scripts on purpose. A miscabled host that reaches `99`
has burned roughly two hours of driver install, DCGM and burn-in before anyone
looks at where it is actually connected.

## What it compares

Two independent sources of truth, joined per cable:

| Side | Source | What it gives |
|---|---|---|
| Planned | NetBox 4.x REST API | For this device, every cabled interface and the far-end `(switch, switch_port)` |
| Actual | `lldpctl` on the host | Per local NIC, the neighbour's chassis name and port ID / description |

The host is identified in NetBox by **DMI system serial** first, falling back to
short hostname. The serial is rejected as unusable if it is blank or one of the
known placeholder strings (`None`, `Not Specified`, `Default string`,
`To be filled by O.E.M.`, `0123456789`, …), which is what an unconfigured
chassis reports.

## Stages

```
preflight        install jq/curl; hard-fail if NETBOX_URL or NETBOX_TOKEN is still CHANGEME
identify_host    DMI serial, else short hostname
enumerate_nics   /sys/class/net, physical only (skips virtio, tun, veth, bridge, bonding, tap)
ensure_lldpd     install + start lldpd, `lldpcli update` to force a TX, then wait
collect_lldp     lldpctl -f json0, normalised to one flat row per interface
fetch_planned    NetBox: device -> interfaces?cabled=true (paginated) -> connected endpoints
compare_and_build  join planned against actual, classify every row
print_table      ASCII planned-vs-actual table to stderr
assemble_report  JSON to stdout; exit 1 on FAIL
```

## Matching

Comparing a switch port name to a Linux interface name is the hard part, so both
sides are normalised before comparison: lowercased, a known prefix stripped
(`gigabitethernet`, `tengigabitethernet`, `ethernet`, `enp`, `eno`, `ens`, `swp`,
`eth`, `xe`, `et`, …, longest first so `gigabitethernet` is not eaten as `gi`),
then reduced to digits, `/`, `.` and `-`. So `Ethernet1/4`, `eth1/4` and
`TenGigE1/4` all normalise to `1/4`.

Port comparison takes an exact match, or — unless `NAME_MATCH_STRICT=true` — one
being a suffix of the other. Switch names match exactly (case-insensitive) or
where the planned name is the short form of an observed FQDN.

### Endpoint mode is the default, and why

With `MATCH_BY_ENDPOINT=true` (the default), a planned cable is satisfied by
**any** LLDP observation matching its `(switch, port)`, regardless of which host
NIC it appeared on. The alternative — requiring the NetBox interface name to
correspond to the Linux interface name — is unreliable in practice, because
NetBox records `eth0`/`eth1` while predictable naming gives the host
`enp33s0f0np0`.

**This catches the neighbour-port case the script is for.** If the cable is in
`Eth1/5` when NetBox says `Eth1/4`, then `Eth1/4` is observed nowhere on the
host and the row fails as `MISSING_LINK`.

**It does not catch a transposed pair.** If NetBox plans NIC-A → `sw1/Eth1/4`
and NIC-B → `sw1/Eth1/5`, and the installer swaps the two ends at the host, both
endpoints are still observed somewhere on the host, so both rows report `OK`.
Detecting that requires `MATCH_BY_ENDPOINT=false`, which then depends on the
host-side name correspondence being trustworthy. See [Known
gaps](#known-gaps).

## Result codes

| Result | Meaning | Severity |
|---|---|---|
| `OK` | Planned endpoint observed | — |
| `MISSING_LINK` | Planned `(switch, port)` not seen anywhere | critical |
| `WRONG_SWITCH` | Neighbour is a different switch (strict mode only) | critical |
| `WRONG_PORT` | Right switch, wrong port (strict mode only) | critical |
| `LOCAL_NIC_NOT_FOUND` | NetBox interface has no matching host NIC (strict mode only) | critical |
| `UNPLANNED_LINK` | NIC is live with a neighbour, but has no cable in NetBox | critical |
| `NO_LLDP_ON_UP_NIC` | NIC has carrier but no neighbour — cannot prove where it lands | critical |
| `UNPLANNED_IDLE` | NIC not in NetBox, no carrier, no neighbour | ignored in endpoint mode |
| `NOT_CHECKED` | BMC/IPMI interface, invisible to the host OS | info |
| `LLDP_SWITCH_OFFLINE` | Fleet-level: NICs are up but zero neighbours anywhere | critical |

A `MISSING_LINK` reason is built to be actionable on its own — it lists NICs that
are down (unplugged cable or dead optic), up NICs with no neighbour (switch-side
LLDP off, or the wrong port), and every endpoint that *was* observed, so a
transposition is visible in the text even when the row-level verdict cannot
classify it.

**The verdict is FAIL if any issue is critical**, and the script exits 1.
Info-severity entries are recorded but do not gate the run.

### Undocumented live links fail by default

`NO_LLDP_ON_UP_NIC` and `UNPLANNED_LINK` are critical because the script cannot
prove where an undocumented live link terminates. A NIC with carrier and no LLDP
is either a switch port with LLDP disabled or a rogue cable, and neither is
something to certify. Set `ALLOW_EXTRA_LINKS=true` to downgrade both to info.

## When no neighbours are seen at all

If the host has at least one NIC up and LLDP returns nothing on any of them,
that is reported once as `LLDP_SWITCH_OFFLINE` rather than as a flurry of
per-row `MISSING_LINK` failures, so the verdict names the actual cause.

The empty case also triggers a diagnostics dump to stderr, which exists to
separate three failure modes that look identical from the host:

1. **Switch-side LLDP disabled** — `lldpcli show statistics` shows TX but no RX.
2. **Mellanox firmware LLDP intercept** — a ConnectX NIC can swallow LLDPDUs in
   firmware before the kernel sees them. If any `mlx5_core` NIC is present the
   script installs `mft` on demand, runs `mst start`, and dumps the
   `LLDP_NB_*` / `DCBX` registers via `mlxconfig` (falling back to querying by
   PCI address when `/dev/mst/*` is not populated). Non-zero means the firmware
   is eating them.
3. **Daemon or timing problem** — `lldpd` not running, or discovery not finished
   inside `LLDP_WAIT_SECONDS`.

It also dumps `/proc/net/dev` multicast RX counters, `ethtool --show-priv-flags`
for mlx5 NICs, and the first 2 KB of raw `lldpctl` JSON so the parse can be
checked against what the daemon actually returned.

## Configuration

Set by editing the script before upload, or via the environment.

| Variable | Default | Effect |
|---|---|---|
| `NETBOX_URL` | `https://netbox.example.com` | **CHANGEME** — preflight fails on the default |
| `NETBOX_TOKEN` | `CHANGEME` | **CHANGEME** — preflight fails on the default |
| `MATCH_BY_ENDPOINT` | `true` | Match on `(switch, port)` only, ignoring host ifname |
| `NAME_MATCH_STRICT` | `false` | Require exact normalised port names; disables suffix matching |
| `ALLOW_EXTRA_LINKS` | `false` | Downgrade undocumented live links to info |
| `IGNORE_IFNAMES` | *(empty)* | Comma-separated NICs to skip entirely |
| `BMC_IFNAME_PATTERN` | `^(ipmi\|bmc\|mgmt\|management)$` | NetBox names treated as BMC, reported `NOT_CHECKED` |
| `LLDP_WAIT_SECONDS` | `45` | Discovery wait after starting `lldpd` |
| `REQUIRED_PKGS` | `jq curl` | Installed up front — the MAAS ephemeral image has neither |

`jq` and `curl` are installed by this script rather than inherited from `90`.
This script sorts ahead of `90`, so relying on it left `80` failing preflight on
every machine, every run.

`emit_fatal` deliberately hand-rolls its JSON instead of using `jq`. The
commonest fatal case is `jq` itself being missing, and a failure handler built on
the missing tool dies with "command not found" and emits nothing — leaving MAAS
with a failure and no verdict explaining it.

## Output

Structured JSON on stdout: `report_metadata` (including `links_ok`,
`links_failing` and a SHA-256 over the collected evidence), `verdict`, `system`,
`netbox` (URL, device id, device name — **not** the token), `planned`,
`actual.nics`, `actual.lldp`, and the per-row `comparison`. The ASCII
planned-vs-actual table and all logging go to stderr, where MAAS keeps them.

## Known gaps

- **A transposed pair passes** in the default endpoint mode. See [Endpoint mode
  is the default](#endpoint-mode-is-the-default-and-why).
- **`REQUIRE_ALL_PLANNED` is dead.** It is defined and documented as config but
  never read; every planned cable is always required. Remove it or wire it up.
- **Not consumed by the report generator.** `device_certificate.py` does not
  read this script's JSON, so the cabling verdict appears only as a MAAS
  commissioning pass/fail and never reaches the certificate.
- **Breakout cables are simplified.** An interface with multiple connected
  endpoints uses the first and logs a warning.
- **The NetBox token lives in the uploaded script.** It is not in
  `.env.example`, and it is not echoed into the JSON, but it is stored in MAAS
  in the script body.
