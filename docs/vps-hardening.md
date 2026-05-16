# VPS Hardening

This project can run on very small VPS shapes, but streaming concurrency can create host memory pressure. The deploy-safe hardening is:

- add a bounded swap file
- keep `vm.swappiness=10`
- run the app container with memory guardrails
- run the resource-aware watchdog every 5 minutes

The default Compose limits are:

```yaml
mem_limit: 768m
memswap_limit: 1536m
```

These values are intended for a 1 GB Oracle E2 Micro style host. Larger VPS shapes can raise them.

## Install

From the deployed app directory:

```bash
chmod +x deploy/vps_hardening.sh deploy/duckdns_watchdog.sh
./deploy/vps_hardening.sh
docker-compose up -d --no-build
```

The script does not read or write secrets. Runtime secrets remain in `config.env`, which must stay untracked.

## Verify

```bash
free -h
swapon --show
docker inspect tg_stremio --format 'mem={{.HostConfig.Memory}} memswap={{.HostConfig.MemorySwap}}'
curl -fsS http://127.0.0.1:8000/login >/dev/null
tail -n 20 /home/ubuntu/duckdns-watchdog.log
```
