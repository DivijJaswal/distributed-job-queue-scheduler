# API Examples

Run the server with `yarn local`, then post these payloads with `curl`.

```bash
curl -sS -X POST http://127.0.0.1:4230/api/jobs -H 'content-type: application/json' --data @examples/enqueue-job.json
curl -sS -X POST http://127.0.0.1:4230/api/workflows -H 'content-type: application/json' --data @examples/workflow-dag.json
curl -sS -X POST http://127.0.0.1:4230/api/schedules -H 'content-type: application/json' --data @examples/cron-schedule.json
curl -sS -X POST http://127.0.0.1:4230/api/rate-limits -H 'content-type: application/json' --data @examples/rate-limit.json
curl -sS -X POST http://127.0.0.1:4230/api/failures/inject -H 'content-type: application/json' --data @examples/failure-injection.json
```
