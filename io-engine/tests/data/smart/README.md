# SMART fixtures

Real captures from physical devices, used by the unit tests in `src/core/smart.rs`.
Serial numbers and WWNs are replaced with placeholders; nothing else is edited
unless noted below.

| File | Device | Command |
|---|---|---|
| `nvme_p4800x_smart_log.bin` | Dell P4800X 375GB NVMe (fw E201DP35) | `nvme get-log /dev/nvme0 --log-id=2 --log-len=512 --raw-binary` (nvme-cli 2.8) |
| `nvme_p4800x_smartctl_all.json` | same NVMe drive | `smartctl --json --all /dev/nvme0n1` (smartctl 7.4) |
| `nvme_970evoplus_smartctl_all.json` | Samsung 970 EVO Plus 500GB NVMe (fw 2B2QEXM7), has a logged error | `smartctl --json --all -l devstat /dev/nvme2n1` (smartctl 7.5) |
| `sas_ssd_smartctl_all.json` | Toshiba KPM5XVUG960G SAS SSD behind a MegaRAID controller | `smartctl -d megaraid,0 --json --all /dev/bus/0` (smartctl 7.4) |
| `sata_ssd_smartctl_x.json` | Crucial CT250MX500SSD1 SATA SSD (fw M3CR023) | `smartctl /dev/sda -AHjx` (smartctl 7.5) |

Notes:

- The values in `nvme_p4800x_smart_log.bin` were checked against the `nvme smart-log` output
  taken at the same time. Counters that tick while the drive is busy (data units
  written, host write commands) differ slightly between the tool outputs, so the
  tests assert those from the page itself. `warning_temp_time` and
  `critical_comp_time` are 0 on this drive, so offsets 192 and 196 are only checked
  against zero.
- smartctl 7.4 does not emit `warning_temp_time` / `critical_comp_time` in the NVMe
  JSON (P4800X capture); smartctl 7.5 does (970 EVO Plus capture, both 0).
- The 970 EVO Plus capture is the only one with an NVMe error-log table.
- The SATA capture was taken with `-x`, which is a superset of what the engine runs
  (`--json --all -l devstat`). The bulky `ata_log_directory` and
  `ata_sct_temperature_history` sections were removed. `spare_available` is absent
  on this model.
