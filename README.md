# XiaoMCP

MVP arsitektur Xiaozhi ↔ OpenClaw yang dipisah jadi:
- **bridge**: edge adapter ringan untuk menerima request, membuat job, dan bisa langsung konek websocket native ke Xiaozhi
- **orchestrator**: API + state job + notifikasi Telegram
- **worker**: proses outbound dari mesin yang punya OpenClaw

## Status
Versi ini sudah **bisa dipakai sebagai MVP** untuk:
- membuat job async,
- claim job oleh worker,
- update progress/result,
- cek result terakhir,
- kirim hasil selesai ke Telegram.

Catatan penting:
- `bridge` sekarang sudah bisa jalan sebagai **websocket-native Xiaozhi bridge** memakai `XIAOZHI_WS_URL`.
- selain mode websocket native, bridge tetap menyediakan endpoint HTTP untuk debug dan adapter lain.

## Komponen

### 1. Orchestrator
Endpoint utama:
- `POST /v1/jobs`
- `GET /v1/jobs/:id`
- `GET /v1/jobs/latest`
- `POST /v1/worker/heartbeat`
- `POST /v1/worker/claim-next`
- `PATCH /v1/jobs/:id`

### 2. Bridge
Mode:
- **WebSocket native Xiaozhi** via `XIAOZHI_WS_URL`
- **HTTP debug/adapter mode**

Endpoint:
- `GET /tools`
- `POST /reload-workflows`
- `POST /invoke/start_workflow`
- `GET /invoke/get_last_result`
- `GET /invoke/job/:id`

Workflow tool names bisa diatur lewat file JSON (`bridge/workflows.json`, lihat `bridge/workflows.example.json`).

### 3. Worker
Mode:
- `EXECUTION_MODE=demo` → hanya simulasi
- `EXECUTION_MODE=openclaw` → jalankan command OpenClaw nyata

## Quick start VPS

### 1. Jalankan orchestrator + postgres + bridge
```bash
cp orchestrator/.env.example orchestrator/.env
cp bridge/.env.example bridge/.env
cp bridge/workflows.example.json bridge/workflows.json
cp .env.example .env 2>/dev/null || true
docker compose up -d --build
```

Atau minimal siapkan env shell:
```bash
export API_KEY=change-me
export DB_USER=postgres
export DB_PASSWORD=postgres
export DB_NAME=xo_db
```

### 2. Jalankan migration prisma di container orchestrator
```bash
docker compose exec orchestrator npx prisma migrate dev --name init
```

### 3. Test create job lewat bridge
```bash
curl -X POST http://localhost:3100/invoke/start_workflow \
  -H 'content-type: application/json' \
  -d '{"workflow":"openclaw_chat","text":"halo dari xiaozhi"}'
```

## Quick start worker lokal
Di mesin yang ada OpenClaw:

```bash
cd worker
cp .env.example .env
npm install
npm run build
npm start
```

### Mode demo
Biarkan:
```env
EXECUTION_MODE=demo
```

### Mode OpenClaw nyata
Set misalnya:
```env
EXECUTION_MODE=openclaw
OPENCLAW_CMD=openclaw
OPENCLAW_ARGS_TEMPLATE=agent --json --message {{text}}
```

> Kalau nanti perlu format command lebih spesifik per workflow, paling bagus tambahkan router workflow di worker.

## Contoh alur
### Mode websocket native Xiaozhi
1. Bridge connect ke `XIAOZHI_WS_URL`.
2. Xiaozhi meminta `tools/list` lalu `tools/call`.
3. Bridge membuat job ke orchestrator.
4. Worker lokal claim job.
5. Worker menjalankan OpenClaw.
6. Worker update result.
7. Orchestrator kirim ringkasan selesai ke Telegram.
8. Jika user minta baca hasil lagi, tool `readToolName` akan membaca status/result terbaru dari orchestrator.

### Mode HTTP adapter
1. Adapter eksternal memanggil `POST /invoke/start_workflow`.
2. Alurnya sama seperti di atas.

## Kenapa model ini enak
- reinstall OpenClaw tidak merusak state job,
- worker bisa mati-hidup tanpa kehilangan antrian,
- VPS hanya opsional jika mau multi-user / external users,
- state tidak lagi bergantung file `.md`.

## Yang masih layak ditingkatkan
- auth worker per-device,
- queue yang lebih kuat,
- claim job dengan lease timeout,
- dashboard admin,
- push/proactive result balik ke device Xiaozhi tanpa menunggu user minta baca lagi,
- routing workflow nyata ke command OpenClaw yang berbeda.
