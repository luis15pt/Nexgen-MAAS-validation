#!/usr/bin/env python3
"""Certificate of Sanitisation from 95-nexgen-storage-sanitize output.

A separate document from the GPU commissioning certificate, deliberately. A
decommission is not a commissioning: the subject is the media rather than the
accelerators, the verdict answers "may this host leave NexGen custody" rather
than "is this hardware fit to deliver", and the reader is whoever signs the
disposal off. Nothing in device_certificate.py's REQUIRED_STAGES or
evaluate_gpu_acceptance has anything to say about either question.

What it shares with that module is reused rather than reimplemented: the raw-log
embedding, the base64 decode, the verdict badge, the file loader and the MAAS
client all come from it by import.

The document covers sections 6 to 10 of the Remove Host for Sale runbook and
states plainly what it does not cover. Section 14 requires an independent
reviewer, so the checklist renders unticked and the reviewer field stays null:
this script cannot be its own second engineer.

Must stay importable and runnable on Python 3.8, which is the oldest we deploy
to -- hence `from __future__ import annotations` and no str.removesuffix.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from html import escape

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import device_certificate as dc  # noqa: E402

__version__ = "1.0.0"

SCRIPT_NAME = "95-nexgen-storage-sanitize.sh"

# The runbook sections this document evidences, and the ones it does not.
COVERS = "media sanitisation to NIST SP 800-88r1"
RUNBOOK = "Remove Host for Sale — Secure Decommissioning Procedure"

# SP 800-88r1's own words, because the distinction is the whole point of the
# document and a reader should not have to take it on trust.
NIST_NOTE = (
    "NIST SP 800-88r1 defines <b>Clear</b> as logical techniques over all "
    "user-addressable locations, resisting simple non-invasive recovery, and "
    "<b>Purge</b> as techniques that render recovery infeasible using "
    "state-of-the-art laboratory techniques. On flash media an overwrite reaches "
    "only user-addressable blocks, so over-provisioned capacity, retired blocks "
    "and unmapped FTL copies fall outside it — an overwrite is therefore a Clear. "
    "A Purge on flash is a firmware operation: ATA or NVMe SANITIZE, or a "
    "cryptographic erase. Because the required level depends on whether the media "
    "leaves organisational control, <b>Purge is the level required here</b>."
)

# Section 14's checklist. Rendered unticked: every line is a human judgement,
# and several concern systems this script never touches.
METHOD_LABELS = {
    "nvme-sanitize-crypto":  "NVMe Sanitize — cryptographic erase",
    "nvme-sanitize-block":   "NVMe Sanitize — block erase",
    "nvme-format-ses2":      "NVMe Format — secure erase (SES=2)",
    "ata-sanitize-crypto":   "ATA Sanitize — crypto scramble",
    "ata-sanitize-block":    "ATA Sanitize — block erase",
    "scsi-sanitize-crypto":  "SCSI Sanitize — cryptographic erase",
    "raid-cli-secure-erase": "RAID controller secure erase",
    "overwrite":             "Full-device overwrite",
    "none":                  "None available",
    "not-applicable":        "Not applicable",
}

DEVICE_TYPE_LABELS = {
    "nvme":              "NVMe SSD",
    "bmc-virtual-media": "BMC virtual media (emulated, not in the chassis)",
    "raid-member": "Drive behind a RAID controller",
    "raid-volume": "RAID virtual disk",
    "sata":        "SATA SSD",
    "sata-hdd":    "SATA hard disk",
    "sas":         "SAS drive",
    "usb":         "USB boot media",
    "sd":          "SD boot media",
    "unknown":     "Storage device",
}


# ---------------------------------------------------------------------------
# Small formatting helpers. `cell` exists because None and "" are different
# claims in the source data and must stay different in the document: a blank
# serial reads as "this part has no serial", which is a statement of fact the
# evidence cannot make.
# ---------------------------------------------------------------------------
def cell(value, mono: bool = False) -> str:
    if value is None or value == "":
        return '<span class="dim">not recorded</span>'
    text = escape(str(value))
    return '<span class="mono">%s</span>' % text if mono else text


def human_bytes(n) -> str:
    if not isinstance(n, (int, float)) or n <= 0:
        return '<span class="dim">--</span>'
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if n < 1000:
            return "%.1f %s" % (n, unit) if unit not in ("B", "KB") else "%d %s" % (n, unit)
        n /= 1000.0
    return "%.1f EB" % n


def result_badge(result: str, level: str) -> str:
    """The result and the level it was achieved at, as one claim.

    A success at level "clear" is not the same claim as a success at level
    "purge", and collapsing them is exactly the confusion this document exists
    to prevent.
    """
    r = (result or "").lower()
    # The absence of a method outranks the outcome. On an audit run every row
    # reads "not attempted", and for a drive that cannot be purged at all that
    # is true but beside the point: what the reader needs to see is that no
    # approved method exists for it.
    if level == "n/a":
        return '<span class="badge badge-na">N/A</span>'
    if level == "none" or r == "no method available":
        return '<span class="badge badge-fail">NOT PURGED</span>'
    if r == "success" and level == "purge":
        return '<span class="badge badge-pass">PURGED</span>'
    if r == "success" and level == "clear":
        return '<span class="badge badge-warn">CLEARED ONLY</span>'
    if r == "not applicable":
        return '<span class="badge badge-na">N/A</span>'
    if r in ("failed", "timeout", "unverified"):
        return '<span class="badge badge-fail">%s</span>' % escape(r.upper())
    if r == "not attempted":
        return '<span class="badge badge-na">NOT ATTEMPTED</span>'
    return '<span class="badge badge-na">%s</span>' % escape((result or "?").upper())


def yesno(value, yes="Yes", no="No") -> str:
    if value is True:
        return yes
    if value is False:
        return '<span class="bad">%s</span>' % escape(no)
    return '<span class="dim">not recorded</span>'


# ---------------------------------------------------------------------------
# Sections
# ---------------------------------------------------------------------------
def render_attestation(data: dict) -> str:
    """Section 6 -- the maintenance environment the erase was performed from."""
    env = data.get("environment") or {}
    if not env:
        return ('<span class="dim">No environment attestation was recorded, so it '
                'cannot be shown that the erase was performed from a trusted '
                'maintenance environment.</span>')

    rows = [
        ("Root filesystem", "%s <span class=\"dim\">on</span> %s"
            % (cell(env.get("root_fstype"), True), cell(env.get("root_source"), True))),
        ("Ephemeral root", yesno(env.get("is_ephemeral"),
                                 "Yes — no sanitised device held the running system",
                                 "No — root was on persistent storage")),
        ("Kernel", cell(env.get("kernel"), True)),
        ("nvme-cli", cell(env.get("nvme_cli_version"), True)),
        ("Sanitise log read as JSON", yesno(env.get("nvme_sanitize_log_json"),
                                            "Yes — status adjudicated from the numeric field",
                                            "No — status parsed from the text log")),
        ("RAID CLI staged", cell(env.get("raid_cli"), True) if env.get("raid_cli")
            else '<span class="dim">none — not in the Ubuntu archive and never fetched</span>'),
        ("Environment markers", cell(env.get("maas_indicators"), True)),
    ]
    body = "".join('<tr><th>%s</th><td>%s</td></tr>' % (escape(k), val) for k, val in rows)
    note = escape(env.get("attestation") or "")
    cls = "note" if env.get("is_ephemeral") else "note note-bad"
    return ('<table class="kv">%s</table><p class="%s">%s</p>'
            % (body, cls, note))


def render_inventory(data: dict) -> str:
    """Section 7 -- what was found, stated as a count so a gap is visible."""
    counts = (data.get("inventory") or {}).get("counts") or {}
    if not counts:
        return '<span class="dim">No inventory counts were recorded.</span>'

    tiles = [
        ("Physical media", counts.get("physical_media"), ""),
        ("NVMe controllers", counts.get("nvme_controllers"), ""),
        ("Behind a RAID controller", counts.get("raid_members"), ""),
        ("Other media", counts.get("other_media"), ""),
        ("Purge planned", counts.get("purge_planned"), ""),
        ("Purged", counts.get("purged"), ""),
    ]
    html = ['<div class="tiles">']
    for label, value, _unit in tiles:
        html.append('<div class="tile"><div class="tile-n">%s</div>'
                    '<div class="tile-l">%s</div></div>'
                    % (escape(str(value if value is not None else "--")), escape(label)))
    html.append("</div>")

    nomethod = counts.get("no_method") or 0
    if nomethod:
        html.append('<p class="note note-bad">%d device(s) could not be purged from '
                    'this environment. That is usually a reachability problem rather '
                    'than an unpurgeable drive &mdash; see the remediation register '
                    'below for the routes to a Purge. The host is not cleared for '
                    'release until every device reaches Purge.</p>' % nomethod)
    return "".join(html)


def render_evidence_table(data: dict) -> str:
    """Section 10 -- one row per physical device, every column the runbook asks for."""
    media = [m for m in (data.get("media") or []) if m.get("physical_media")]
    virtual = [m for m in (data.get("media") or []) if not m.get("physical_media")]
    if not media:
        return ('<span class="dim">No physical media were recorded, so there is '
                'no sanitisation evidence to show.</span>')

    head = ("<tr><th>Device</th><th>Make and model</th><th>Serial / WWN</th>"
            "<th>Firmware</th><th>Capacity</th><th>Method</th>"
            "<th>Completed (UTC)</th><th>Result</th><th>Verification</th></tr>")

    rows = []
    for m in media:
        path = m.get("device") or ""
        via = m.get("reached_via") or ""
        dev = escape(path)
        if via and via not in ("direct", "nvme-controller"):
            dev += ' <span class="dim">%s</span>' % escape(via)

        ident = cell(m.get("serial_number"), True)
        if m.get("wwn"):
            ident += '<br><span class="dim mono">%s</span>' % escape(str(m["wwn"]))

        method = METHOD_LABELS.get(m.get("method"), m.get("method") or "?")
        level = m.get("method_level") or ""
        method_html = escape(method)
        if level in ("purge", "clear"):
            cls = "lvl-purge" if level == "purge" else "lvl-clear"
            method_html += ' <span class="%s">NIST %s</span>' % (cls, escape(level))
        if m.get("command"):
            method_html += ('<br><span class="dim mono">%s</span>'
                            % escape(str(m["command"])))

        when = cell(m.get("completed_at"), True)
        if m.get("started_at"):
            when += ('<br><span class="dim mono">from %s</span>'
                     % escape(str(m["started_at"])))

        verif = m.get("verification") or {}
        vbits = []
        if verif.get("serial_matches") is True:
            vbits.append("serial re-read matches")
        elif verif.get("serial_matches") is False:
            vbits.append('<span class="bad">serial re-read DIFFERS</span>')
        if verif.get("residual_filesystem_signature"):
            vbits.append("signature: %s" % escape(str(verif["residual_filesystem_signature"])))
        elif verif:
            vbits.append("no filesystem signature")
        vhtml = "<br>".join(vbits) if vbits else '<span class="dim">--</span>'

        rows.append(
            "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td>"
            "<td class=\"cmd\">%s</td><td>%s</td><td>%s</td><td class=\"vf\">%s</td></tr>"
            % (dev,
               cell(" ".join(x for x in (m.get("manufacturer"), m.get("model")) if x)),
               ident,
               cell(m.get("firmware"), True),
               human_bytes(m.get("capacity_bytes")),
               method_html,
               when,
               result_badge(m.get("result"), level),
               vhtml))

    out = ['<div class="scroll"><table class="ev">%s%s</table></div>' % (head, "".join(rows))]

    reasons = [(m.get("device"), m.get("reached_via"), m.get("method_reason"))
               for m in media if m.get("method_reason")]
    if reasons:
        out.append('<details class="why"><summary>Why each method was chosen'
                   '<span class="ev-meta">%d device(s)</span></summary><table class="kv">'
                   % len(reasons))
        for path, via, why in reasons:
            label = path if via in ("direct", "nvme-controller", None) else "%s %s" % (path, via)
            out.append("<tr><th>%s</th><td>%s</td></tr>" % (escape(str(label)), escape(str(why))))
        out.append("</table></details>")

    if virtual:
        out.append('<p class="note">%d virtual disk(s) presented by a RAID '
                   'controller are recorded in the evidence but are not physical '
                   'media: the drives inside them are listed above in their own '
                   'right.</p>' % len(virtual))

    note = ("A screenshot alone is not evidence. Every row above comes from the "
            "device's own response, and the raw command output is embedded at the "
            "foot of this certificate.")
    out.append('<p class="note">%s</p>' % escape(note))
    return "".join(out)


def render_remediation(data: dict) -> str:
    """Drives that have not reached Purge, and how to get them there.

    Never a destruction list. A drive that resists sanitisation from inside a
    RAID volume is almost always a reachability problem, not an unpurgeable
    drive, so what belongs here is the route to a Purge.
    """
    rem = data.get("remediation") or []
    if not rem:
        return ('<p class="note note-ok">No remediation outstanding. Every physical '
                'device reached NIST Purge and was verified.</p>')

    out = []
    for m in rem:
        via = m.get("reached_via") or ""
        dev = escape(str(m.get("device") or ""))
        if via and via not in ("direct", "nvme-controller"):
            dev += ' <span class="dim">%s</span>' % escape(via)

        sec = m.get("ata_security_supported")
        if sec is True:
            seclabel = ('<span class="ok">the drive reports the ATA Security feature set, '
                        'so an erase issued with it presented directly reaches Purge</span>')
        elif sec is False:
            seclabel = "the drive does not report the ATA Security feature set"
        else:
            seclabel = ('<span class="dim">the feature set could not be read through this '
                        'controller — unproven, not absent</span>')

        rows = []
        for r in m.get("routes") or []:
            reaches = r.get("reaches") or ""
            cls = "lvl-purge" if reaches == "purge" else "lvl-clear"
            bits = ['<div class="rt-h"><span class="rt-n">%s</span>%s '
                    '<span class="%s">reaches NIST %s</span></div>'
                    % (escape(str(r.get("order", ""))), escape(str(r.get("action") or "")),
                       cls, escape(reaches))]
            if r.get("prerequisite"):
                bits.append('<div class="rt-d">First: %s</div>' % escape(str(r["prerequisite"])))
            if r.get("requires"):
                bits.append('<div class="rt-d">Requires: %s</div>' % escape(str(r["requires"])))
            if r.get("survey_first"):
                bits.append('<div class="rt-d">Check first: <span class="mono">%s</span></div>'
                            % escape(str(r["survey_first"])))
            if r.get("command"):
                bits.append('<div class="rt-d">Command: <span class="mono">%s</span></div>'
                            % escape(str(r["command"])))
            if r.get("sequence"):
                bits.append('<div class="rt-d">In order:</div><ol class="seq">%s</ol>'
                            % "".join('<li><span class="mono">%s</span></li>' % escape(str(x))
                                      for x in r["sequence"]))
            if r.get("caveat"):
                bits.append('<div class="rt-c">%s</div>' % escape(str(r["caveat"])))
            rows.append('<li>%s</li>' % "".join(bits))

        out.append(
            '<div class="rem">'
            '<div class="rem-h">%s &middot; <span class="mono">%s</span> &middot; %s'
            '<span class="rem-lvl">reached: NIST %s</span></div>'
            '<div class="rem-b">%s</div>'
            '<div class="rem-s">%s</div>'
            '<ol class="routes">%s</ol>'
            '</div>'
            % (dev, escape(str(m.get("serial_number") or "no serial")),
               escape(str(m.get("model") or "")),
               escape(str(m.get("nist_level_reached") or "none")),
               escape(str(m.get("blocker") or "")),
               seclabel,
               "".join(rows)))

    out.append('<p class="note note-bad">The host is not cleared for release until '
               'every device above reaches NIST Purge. Record the completed erase '
               'against each serial and regenerate this certificate.</p>')
    return "".join(out)


def render_controller_survey(data: dict) -> str:
    """What is blocking the drives a controller hides, established by reading."""
    sv = data.get("controller_survey") or {}
    if not sv:
        return '<span class="dim">No controller survey was recorded.</span>'
    if not sv.get("members_seen"):
        return ('<p class="note">No drives behind a RAID controller on this machine, '
                'so nothing to survey.</p>')

    rows = [
        ("Drives behind a controller", cell(sv.get("members_seen"))),
        ("Controller (PCI)", cell(sv.get("raid_controllers_pci"), True)),
        ("Vendor CLI available", cell(sv.get("raid_cli"), True) if sv.get("raid_cli")
            else '<span class="dim">none staged — not in the Ubuntu archive, and never fetched by the script</span>'),
    ]
    body = "".join('<tr><th>%s</th><td>%s</td></tr>' % (escape(k), v) for k, v in rows)

    out = ['<table class="kv">%s</table>' % body]
    if sv.get("note"):
        out.append('<p class="note">%s</p>' % escape(str(sv["note"])))

    todo = sv.get("to_complete_the_picture") or []
    if todo:
        out.append('<p class="note note-warn">Read-only commands that would complete '
                   'the picture:</p><ul class="oos">%s</ul>'
                   % "".join('<li><span class="mono">%s</span></li>' % escape(str(x))
                             for x in todo))
    return "".join(out)


def render_findings(data: dict) -> str:
    issues = (data.get("verdict") or {}).get("issues") or []
    if not issues:
        return '<p class="note note-ok">No findings.</p>'
    order = {"critical": 0, "warning": 1, "info": 2}
    issues = sorted(issues, key=lambda i: order.get(i.get("severity"), 3))
    rows = []
    for i in issues:
        sev = (i.get("severity") or "info").lower()
        rows.append('<tr class="sev-%s"><td class="sev">%s</td><td>%s</td><td>%s</td></tr>'
                    % (escape(sev), escape(sev.upper()),
                       escape(str(i.get("issue") or "")),
                       escape(str(i.get("details") or ""))))
    return ('<div class="scroll"><table class="ev"><tr><th>Severity</th>'
            '<th>Finding</th><th>Detail</th></tr>%s</table></div>' % "".join(rows))


def render_raw(data: dict) -> str:
    """The logs. Embedded, never linked: whoever reads this is not assumed to
    have access to the MAAS that produced it."""
    inv = data.get("inventory") or {}
    blocks = [
        ("Sanitisation audit log", data.get("audit_log_b64"),
         "every command issued, with UTC timestamps"),
        ("Block-device inventory (lsblk)", inv.get("lsblk_b64"), ""),
        ("NVMe namespace list", inv.get("nvme_list_b64"), ""),
        ("NVMe controller identity and capabilities", inv.get("nvme_id_ctrl_b64"),
         "kept as evidence; the numeric SANICAP and FNA fields are what the method choice used"),
    ]
    for m in data.get("media") or []:
        if m.get("evidence_b64"):
            via = m.get("reached_via") or ""
            label = "%s%s — %s" % (
                m.get("device"),
                "" if via in ("direct", "nvme-controller") else " " + via,
                m.get("serial_number") or "no serial")
            blocks.append(("Device log: " + label, m.get("evidence_b64"), ""))

    out = []
    for title, blob, note in blocks:
        text = dc.decode_b64_text(blob)
        if text.strip():
            out.append(dc.render_log_block(title, text, note=note, keep="tail"))
    if not out:
        return '<span class="dim">No raw logs were captured.</span>'
    return "".join(out)


# ---------------------------------------------------------------------------
# The document
# ---------------------------------------------------------------------------
def generate_certificate(data: dict, hostname_override: str = None) -> str:
    if not data:
        raise ValueError("no sanitisation evidence to render")

    meta = data.get("report_metadata") or {}
    verdict = (data.get("verdict") or {}).get("overall") or "N/A"
    att = data.get("attestation") or {}
    env = data.get("environment") or {}

    host = hostname_override or att.get("hostname") or env.get("hostname") or "unknown-host"
    serial = att.get("system_serial") or env.get("system_serial")
    mode = meta.get("mode") or "?"

    phys = [m for m in (data.get("media") or []) if m.get("physical_media")]
    purged = [m for m in phys
              if m.get("nist_level") == "purge" and m.get("result") == "success"]
    all_done = bool(phys) and len(purged) == len(phys)

    # The headline says what the document is for, and audit mode is not a
    # sanitisation record however clean its verdict.
    if mode == "audit":
        headline = ("Audit only — no device was modified. This is a capability "
                    "and inventory record, not a sanitisation record.")
        hcls = "note note-warn"
    elif all_done:
        headline = ("All %d storage devices in this machine were purged to NIST "
                    "SP 800-88r1 and verified against their own serial numbers. "
                    "The data is destroyed." % len(phys))
        hcls = "note note-ok"
    else:
        headline = ("This host is NOT cleared for release. See the findings and the "
                    "remediation register below.")
        hcls = "note note-bad"

    q_count = len(data.get("remediation") or [])

    return """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Certificate of Sanitisation &mdash; {host}</title>
<style>
:root {{
  --ff: ui-sans-serif, -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
  --mono: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  --bg:#0a0b0e; --card:#12141a; --card2:#181b23; --line:#262a35;
  --txt:#e6e8ee; --txt2:#aab0be; --txt3:#767d8d;
  --ok:#3fb950; --warn:#d29922; --bad:#f85149; --accent:#58a6ff;
}}
@media (prefers-color-scheme: light) {{
  :root {{
    --bg:#fff; --card:#f7f8fa; --card2:#eef0f3; --line:#d7dae0;
    --txt:#12141a; --txt2:#4c5261; --txt3:#767d8d;
    --ok:#1a7f37; --warn:#9a6700; --bad:#cf222e; --accent:#0969da;
  }}
}}
* {{ box-sizing: border-box; }}
body {{ margin:0; padding:2rem 1.25rem 4rem; font-family:var(--ff); font-size:14px;
       background:var(--bg); color:var(--txt); line-height:1.5; }}
.wrap {{ max-width:1400px; margin:0 auto; }}
h1 {{ font-size:1.5rem; margin:0 0 .25rem; letter-spacing:-.01em; }}
.sub {{ color:var(--txt2); font-size:.82rem; margin:0 0 1.5rem; }}
.sub .mono {{ color:var(--txt); }}
section {{ background:var(--card); border:1px solid var(--line); border-radius:10px;
          padding:1.1rem 1.25rem; margin:0 0 1.1rem; }}
.section-label {{ font-size:.7rem; text-transform:uppercase; letter-spacing:.09em;
                 color:var(--txt3); margin:0 0 .8rem; font-weight:600; }}
.badge {{ display:inline-block; padding:.14rem .5rem; border-radius:5px;
         font-size:.68rem; font-weight:700; letter-spacing:.04em; font-family:var(--mono); }}
.badge-pass {{ background:rgba(63,185,80,.15); color:var(--ok); border:1px solid rgba(63,185,80,.4); }}
.badge-warn {{ background:rgba(210,153,34,.15); color:var(--warn); border:1px solid rgba(210,153,34,.4); }}
.badge-fail {{ background:rgba(248,81,73,.15); color:var(--bad); border:1px solid rgba(248,81,73,.4); }}
.badge-na   {{ background:var(--card2); color:var(--txt3); border:1px solid var(--line); }}
.hero {{ display:flex; align-items:center; gap:.75rem; flex-wrap:wrap; margin-bottom:.6rem; }}
.hero .badge {{ font-size:.9rem; padding:.3rem .8rem; }}
table {{ border-collapse:collapse; width:100%; font-size:.78rem; }}
th, td {{ text-align:left; padding:.42rem .55rem; border-bottom:1px solid var(--line);
         vertical-align:top; }}
th {{ color:var(--txt2); font-weight:600; font-size:.72rem; }}
.kv th {{ width:16rem; white-space:nowrap; color:var(--txt2); }}
.kv td {{ color:var(--txt); }}
.scroll {{ overflow-x:auto; }}
.ev td, .ev th {{ white-space:nowrap; }}
.ev td.cmd, .ev td.vf {{ white-space:normal; max-width:22rem; }}
.mono {{ font-family:var(--mono); font-size:.74rem; }}
.dim {{ color:var(--txt3); }}
.bad {{ color:var(--bad); font-weight:600; }}
.lvl-purge {{ color:var(--ok); font-family:var(--mono); font-size:.68rem; }}
.lvl-clear {{ color:var(--warn); font-family:var(--mono); font-size:.68rem; }}
.note {{ font-size:.78rem; color:var(--txt2); margin:.85rem 0 0;
        border-left:3px solid var(--line); padding-left:.7rem; }}
.note-ok {{ border-left-color:var(--ok); }}
.note-warn {{ border-left-color:var(--warn); }}
.note-bad {{ border-left-color:var(--bad); color:var(--txt); }}
.tiles {{ display:flex; flex-wrap:wrap; gap:.6rem; }}
.tile {{ background:var(--card2); border:1px solid var(--line); border-radius:8px;
        padding:.6rem .9rem; min-width:9rem; }}
.tile-n {{ font-size:1.35rem; font-weight:650; font-family:var(--mono); }}
.tile-l {{ font-size:.68rem; color:var(--txt3); text-transform:uppercase;
          letter-spacing:.05em; margin-top:.15rem; }}
.sev {{ font-family:var(--mono); font-size:.68rem; font-weight:700; }}
.sev-critical .sev {{ color:var(--bad); }}
.sev-warning .sev {{ color:var(--warn); }}
.sev-info .sev {{ color:var(--txt3); }}
.oos {{ font-size:.75rem; color:var(--txt2); margin:.4rem 0 0; padding-left:1.1rem; }}
.why, .ev-block {{ margin-top:.8rem; background:var(--card2); border:1px solid var(--line);
                  border-radius:8px; padding:.5rem .7rem; }}
.why summary, .ev-block summary {{ cursor:pointer; font-size:.78rem; font-weight:600; }}
.ev-meta {{ font-family:var(--mono); font-size:.68rem; color:var(--txt3); margin-left:.5rem;
           font-weight:400; }}
.ev-trunc {{ font-family:var(--mono); font-size:.68rem; color:var(--warn); margin-left:.5rem; }}
.ev-pre {{ font-family:var(--mono); font-size:.7rem; white-space:pre-wrap; word-break:break-word;
          max-height:32rem; overflow:auto; background:var(--bg); border:1px solid var(--line);
          border-radius:6px; padding:.6rem; margin:.6rem 0 0; }}
.ok {{ color:var(--ok); }}
.rem {{ background:var(--card2); border:1px solid var(--line); border-radius:8px;
       padding:.7rem .85rem; margin:0 0 .7rem; }}
.rem-h {{ font-size:.8rem; font-weight:600; }}
.rem-lvl {{ float:right; font-family:var(--mono); font-size:.68rem; color:var(--bad); }}
.rem-b {{ font-size:.76rem; color:var(--txt2); margin:.35rem 0; }}
.rem-s {{ font-size:.73rem; color:var(--txt2); margin:0 0 .4rem; }}
.routes {{ margin:.2rem 0 0; padding-left:1.1rem; font-size:.76rem; }}
.routes li {{ margin:.35rem 0; }}
.rt-h {{ font-weight:600; }}
.rt-n {{ display:inline-block; min-width:1.3rem; color:var(--txt3); font-family:var(--mono); }}
.rt-d {{ color:var(--txt2); font-size:.73rem; margin-left:1.3rem; }}
.seq {{ margin:.25rem 0 .25rem 1.3rem; padding-left:1rem; font-size:.72rem; }}
.seq li {{ margin:.15rem 0; }}
.rt-c {{ color:var(--warn); font-size:.73rem; margin-left:1.3rem; margin-top:.2rem; }}
footer {{ color:var(--txt3); font-size:.7rem; text-align:center; margin-top:1.5rem; }}
</style></head><body><div class="wrap">

<h1>Certificate of Sanitisation</h1>
<p class="sub"><span class="mono">{host}</span>{serial_bit} &middot; {covers} of the
{runbook} &middot; generated {generated}</p>

<section>
  <div class="hero">{badge}
    <span class="mono dim">{host}</span>
    <span class="mono dim">{serial_plain}</span>
    <span class="mono dim">{generated}</span>
  </div>
  <p class="{hcls}">{headline}</p>
</section>

<section><div class="section-label">What was destroyed</div>
{evidence}</section>

{outstanding}

<details class="why"><summary>Technical detail<span class="ev-meta">environment,
inventory, method reasoning, controller survey</span></summary>
<p class="note">{nist}</p>
<div class="section-label">Maintenance environment</div>{attestation}
<div class="section-label">Inventory</div>{inventory}
<div class="section-label">Controller survey</div>{survey}
<div class="section-label">Findings</div>{findings}
</details>

<section><div class="section-label">Raw output</div>
<p class="note">Embedded in full, not linked &mdash; the evidence is in this file
or it is not evidence.</p>
{raw}</section>

<footer>Generated by sanitisation_certificate.py v{cert_ver} &middot;
evidence from {script}</footer>
</div></body></html>
""".format(
        host=escape(host),
        host_c=cell(host, True),
        serial_bit=(" &middot; <span class=\"mono\">%s</span>" % escape(str(serial))) if serial else "",
        serial_c=cell(serial, True),
        serial_plain=escape(str(serial or "serial not recorded")),
        covers=escape(COVERS),
        runbook=escape(RUNBOOK),
        generated=escape(str(meta.get("generated_at") or "")),
        generated_c=cell(meta.get("generated_at"), True),
        duration=dc.fmt_dur(meta.get("duration_seconds")),
        badge=(dc.badge("N/A") if mode == "audit"
               else dc.badge("PASS") if all_done else dc.badge("FAIL")),
        mode=escape(mode),
        q_count=q_count,
        hcls=hcls,
        nist=NIST_NOTE,
        headline=escape(headline),
        ticket=cell(att.get("jira_ticket")),
        engineer=cell(att.get("engineer")),
        script=escape(SCRIPT_NAME),
        ver=escape(str(meta.get("script_version") or "?")),
        hash=cell(meta.get("data_hash_sha256"), True),
        findings=render_findings(data),
        attestation=render_attestation(data),
        inventory=render_inventory(data),
        evidence=render_evidence_table(data),
        outstanding=("" if all_done else
                     '<section><div class="section-label">Outstanding &mdash; '
                     'not yet purged</div>%s</section>' % render_remediation(data)),
        survey=render_controller_survey(data),
        raw=render_raw(data),
        cert_ver=escape(__version__),
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    p = argparse.ArgumentParser(
        prog="sanitisation_certificate.py",
        description="Certificate of Sanitisation from 95-nexgen-storage-sanitize output.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""examples:
  # from a file, or from stdin with -
  sanitisation_certificate.py --sanitize 95-output.json -o cert.html

  # straight from MAAS
  sanitisation_certificate.py --host EXAMPLE-NODE-01 --maas-url $MAAS_URL
""")
    p.add_argument("--sanitize", metavar="FILE",
                   help="95-nexgen-storage-sanitize JSON file, or - for stdin")
    p.add_argument("--host", metavar="HOSTNAME", help="fetch the evidence from MAAS")
    p.add_argument("--maas-url", metavar="URL", help="MAAS URL (default: $MAAS_URL)")
    p.add_argument("--api-key", metavar="KEY", help="MAAS API key (default: $MAAS_API_KEY)")
    p.add_argument("--hostname", metavar="NAME", help="override the hostname on the certificate")
    p.add_argument("--output", "-o", metavar="FILE", help="output HTML file (default: stdout)")
    p.add_argument("--quiet", "-q", action="store_true", help="suppress progress messages")
    p.add_argument("--version", action="version", version="%(prog)s " + __version__)
    args = p.parse_args()

    dc._quiet = args.quiet

    if not args.sanitize and not args.host:
        p.error("give --sanitize FILE or --host HOSTNAME")

    if args.host:
        maas_url = args.maas_url or os.environ.get("MAAS_URL", "")
        api_key = args.api_key or os.environ.get("MAAS_API_KEY", "")
        if not maas_url:
            p.error("--host requires a MAAS URL. Set --maas-url or export MAAS_URL=...")
        if not api_key:
            p.error("--host requires a MAAS API key. Set --api-key or export MAAS_API_KEY=...")
        dc.log("Connecting to MAAS at %s" % maas_url)
        client = dc.MAASClient(maas_url, api_key)
        machine = client.resolve_hostname(args.host)
        data = client.get_script_json(machine["system_id"], SCRIPT_NAME)
        if not data:
            print("Error: %s has no output from %s in MAAS"
                  % (args.host, SCRIPT_NAME), file=sys.stderr)
            sys.exit(1)
        host_default = machine.get("fqdn") or args.host
    else:
        data = dc.load_json_file(args.sanitize)
        if not data:
            print("Error: no valid JSON loaded from %s" % args.sanitize, file=sys.stderr)
            sys.exit(1)
        host_default = None

    html = generate_certificate(data, hostname_override=args.hostname or host_default)

    out = args.output
    if not out:
        att = data.get("attestation") or {}
        env = data.get("environment") or {}
        host = (args.hostname or att.get("hostname") or env.get("hostname")
                or "unknown-host")
        out = os.path.join("reports", "%s-sanitisation-certificate.html" % host)

    if out == "-":
        sys.stdout.write(html)
    else:
        d = os.path.dirname(out)
        if d:
            try:
                os.makedirs(d)
            except OSError:
                pass
        with open(out, "w") as fh:
            fh.write(html)
        dc.log("Wrote %s (%d bytes)" % (out, len(html)))


if __name__ == "__main__":
    main()
