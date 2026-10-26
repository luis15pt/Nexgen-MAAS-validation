# Adding Scripts to MAAS

[← Back to README](../README.md)

> **The metadata comment block is documentation only — MAAS does not parse it.**
> It uses `# --- Start MAAS Metadata ---`, and MAAS matches
> `# --- Start MAAS 1.0 script metadata ---`, so MAAS ignores the block and takes
> `name`, `script_type` and everything else from the upload command.
>
> That is deliberate, and was arrived at the hard way. Switching to the delimiter
> MAAS parses made MAAS start *validating* the block, and uploads stopped working
> against MAAS 3.1.4 in this environment — first with *"May not override values
> defined in embedded YAML"* when a supplied `name=` differed from the embedded
> one, then with *"This field is required"* when the embedded `name` was removed,
> and then still failing once both were consistent. Reverting to the ignored
> delimiter restored working uploads. Which key MAAS 3.1.4 objected to was never
> established.
>
> **Consequence worth knowing:** the declared `timeout` values are therefore *not*
> enforced by MAAS. `Script.timeout` defaults to 0, which means no timeout. The
> real protection is the in-script `timeout` wrappers around `dcgmi diag` and the
> load generator, which bound the two phases that can actually hang. Titles and
> descriptions likewise do not appear in the MAAS UI.
>
> `tests/validate-maas-metadata.py` checks the block is internally consistent and
> that `name` matches the filename — a stale name silently registers a second
> script — and fails if the live MAAS delimiter reappears.

Upload the commissioning scripts via the MAAS CLI. MAAS does not inject
per-script configuration, so anything a script needs is a default inside it:
`BURN_DURATION` in particular sets total commissioning time and has to be edited
before upload.

```bash
maas $PROFILE commissioning-scripts create \
  name=89-nexgen-hw-inventory \
  script_type=commissioning \
  hardware_type=node \
  content@=commissioning-scripts/89-nexgen-hw-inventory.sh

maas $PROFILE commissioning-scripts create \
  name=90-nexgen-gpu-install-595-13 \
  script_type=commissioning \
  hardware_type=gpu \
  content@=commissioning-scripts/90-nexgen-gpu-install-595-13.sh

maas $PROFILE commissioning-scripts create \
  name=91-nexgen-gpu-mig-ecc-config \
  script_type=commissioning \
  hardware_type=gpu \
  content@=commissioning-scripts/91-nexgen-gpu-mig-ecc-config.sh

maas $PROFILE commissioning-scripts create \
  name=92-nexgen-gpu-inventory \
  script_type=commissioning \
  hardware_type=gpu \
  content@=commissioning-scripts/92-nexgen-gpu-inventory.sh

# The DCGM diagnostic is four scripts, one per level, each refusing to run if
# a lower level failed. Upload the rungs you want; an absent rung is simply
# not consulted. 96 (level 4) runs for hours -- raise the MAAS node timeout
# past its 27600 s ceiling before uploading it.
for rung in 93-nexgen-dcgm-level1 94-nexgen-dcgm-level2 \
            95-nexgen-dcgm-level3 96-nexgen-dcgm-level4; do
  maas $PROFILE commissioning-scripts create \
    name=$rung \
    script_type=commissioning \
    hardware_type=gpu \
    content@=commissioning-scripts/$rung.sh
done

# Optional -- omit this one to skip the sustained burn-in entirely
maas $PROFILE commissioning-scripts create \
  name=99-nexgen-gpu-burn-in \
  script_type=commissioning \
  hardware_type=gpu \
  content@=commissioning-scripts/99-nexgen-gpu-burn-in.sh
```

**Script `70` is uploaded separately, for a decommission only.** It is the one
destructive script here, and it is not part of the commissioning set above:

```bash
# Edit SANITIZE_CONFIRM in the script first, and set EXPECT_SYSTEM_SERIAL to the
# chassis you intend to erase. Without that edit the script runs in audit mode
# and destroys nothing -- which is how you should run it the first time.
maas $PROFILE commissioning-scripts create \
  name=70-nexgen-storage-sanitize \
  script_type=commissioning \
  hardware_type=node \
  content@=commissioning-scripts/70-nexgen-storage-sanitize.sh
```

> **Raise the MAAS node timeout past the expected erase duration first.** Same trap
> as the burn-in: at the 30-minute default MAAS marks the machine Failed while the
> sanitise is still running, and the run's evidence is lost with it. A crypto erase
> is seconds; a block erase or an overwrite is not.
>
> Delete the script from MAAS when the decommission is done. Its default is inert,
> but a destructive script with no current purpose does not need to be sitting in
> the commissioning-script list.
