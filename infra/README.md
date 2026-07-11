# Infrastructure

Step 1 reserves this module without mutating local or remote infrastructure.

Later milestones will add a Compose project named `american_stocks` with Next.js, FastAPI, Celery, PostgreSQL, and Redis. Remote application data will be isolated under:

```text
/mnt/HC_Volume_106309665/american_stocks
```

The remote frontend and API will bind only to `127.0.0.1`. Caddy, domains, legacy containers, and legacy Docker volumes remain untouched until the separately reviewed remote-deployment step.
