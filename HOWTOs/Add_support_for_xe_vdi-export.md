# How to add support for `xe vdi-export`

1. Rename `/sbin/tap-ctl` to `/sbin/tap-ctl-orig`
2. Rename `/bin/vhd-tool` to `/bin/vhd-tool-orig`
3. Download and install `tap-ctl` and `vhd-tool` into `/sbin` and `/bin` respectively.