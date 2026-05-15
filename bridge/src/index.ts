import fs from 'node:fs';
import path from 'node:path';
import Fastify from 'fastify';
import axios from 'axios';
import pino from 'pino';
import dotenv from 'dotenv';
import { z } from 'zod';
import WebSocket from 'ws';

dotenv.config();

const logger = pino({
  transport: {
    target: 'pino-pretty',
    options: { colorize: true }
  }
});

const API_URL = process.env.ORCHESTRATOR_URL || 'http://localhost:3000';
const API_KEY = process.env.API_KEY || 'change-me';
const HOST = process.env.HOST || '0.0.0.0';
const PORT = Number(process.env.PORT || 3100);
const DEFAULT_WORKFLOW = process.env.DEFAULT_WORKFLOW || 'openclaw_chat';
const XIAOZHI_WS_URL = process.env.XIAOZHI_WS_URL || '';
const WORKFLOWS_CONFIG_PATH = process.env.WORKFLOWS_CONFIG_PATH || path.resolve(process.cwd(), 'workflows.json');
const STATE_PATH = process.env.BRIDGE_STATE_PATH || path.resolve(process.cwd(), '.bridge-state.json');
const KEEPALIVE_MS = Number(process.env.KEEPALIVE_MS || 20000);
const RECONNECT_MS = Number(process.env.RECONNECT_MS || 3000);

const client = axios.create({
  baseURL: API_URL,
  headers: { 'x-api-key': API_KEY },
  timeout: 15000,
  validateStatus: () => true,
});

const fastify = Fastify({ logger });

const StartSchema = z.object({
  workflow: z.string().min(1).optional(),
  text: z.string().min(1),
  userId: z.string().optional(),
  correlationId: z.string().optional(),
});

type WorkflowConfig = {
  id: string;
  toolName: string;
  readToolName?: string;
  description?: string;
  readDescription?: string;
  aliases?: string[];
  readAliases?: string[];
};

type BridgeState = {
  lastJobs?: Record<string, { jobId: string; createdAt: string }>;
};

let workflowConfigs: WorkflowConfig[] = [];
const workflowToolMap = new Map<string, { kind: 'run' | 'read'; workflow: WorkflowConfig }>();
let customTools: any[] = [];
let ws: WebSocket | null = null;
let keepAliveTimer: NodeJS.Timeout | null = null;
let reconnectTimer: NodeJS.Timeout | null = null;
let nextPingId = 100000;
let shuttingDown = false;
let toolCallQueue = Promise.resolve();

function ensureParentDir(filePath: string) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
}

function loadBridgeState(): BridgeState {
  try {
    if (!fs.existsSync(STATE_PATH)) return {};
    return JSON.parse(fs.readFileSync(STATE_PATH, 'utf8'));
  } catch (err) {
    logger.warn({ err }, 'Failed to load bridge state');
    return {};
  }
}

function saveBridgeState(patch: Partial<BridgeState>) {
  const previous = loadBridgeState();
  const next = { ...previous, ...patch };
  ensureParentDir(STATE_PATH);
  fs.writeFileSync(STATE_PATH, JSON.stringify(next, null, 2));
}

function rememberJob(workflowId: string, jobId: string) {
  const state = loadBridgeState();
  const lastJobs = { ...(state.lastJobs || {}), [workflowId]: { jobId, createdAt: new Date().toISOString() } };
  saveBridgeState({ lastJobs });
}

function getRememberedJobId(workflowId: string) {
  return loadBridgeState().lastJobs?.[workflowId]?.jobId || null;
}

function rebuildWorkflowMaps() {
  workflowToolMap.clear();
  customTools = [];

  for (const workflow of workflowConfigs) {
    const runNames = [workflow.toolName, ...(workflow.aliases || [])].filter(Boolean);
    const readNames = [workflow.readToolName, ...(workflow.readAliases || [])].filter(Boolean) as string[];

    for (const name of runNames) {
      workflowToolMap.set(name, { kind: 'run', workflow });
      customTools.push({
        name,
        description: workflow.description || `Jalankan workflow ${workflow.id}.`,
        inputSchema: {
          type: 'object',
          properties: {
            text: { type: 'string', minLength: 1 },
          },
          required: ['text'],
        },
      });
    }

    for (const name of readNames) {
      workflowToolMap.set(name, { kind: 'read', workflow });
      customTools.push({
        name,
        description: workflow.readDescription || `Baca hasil/status terakhir workflow ${workflow.id}.`,
        inputSchema: {
          type: 'object',
          properties: {},
          additionalProperties: false,
        },
      });
    }
  }

  if (!customTools.length) {
    customTools = [
      {
        name: 'start_workflow',
        description: 'Membuat job async ke orchestrator dan langsung mengembalikan job id.',
        inputSchema: {
          type: 'object',
          properties: {
            workflow: { type: 'string' },
            text: { type: 'string' }
          },
          required: ['text']
        }
      },
      {
        name: 'get_last_result',
        description: 'Mengambil ringkasan hasil terakhir yang selesai.',
        inputSchema: { type: 'object', properties: {}, additionalProperties: false }
      }
    ];
  }
}

function loadWorkflowConfigs() {
  try {
    if (!fs.existsSync(WORKFLOWS_CONFIG_PATH)) {
      workflowConfigs = [];
      rebuildWorkflowMaps();
      return;
    }
    const parsed = JSON.parse(fs.readFileSync(WORKFLOWS_CONFIG_PATH, 'utf8'));
    workflowConfigs = Array.isArray(parsed) ? parsed.filter(Boolean) : [];
    rebuildWorkflowMaps();
  } catch (err) {
    logger.warn({ err }, 'Failed to load workflow config');
    workflowConfigs = [];
    rebuildWorkflowMaps();
  }
}

loadWorkflowConfigs();

function send(obj: unknown) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    const payload = JSON.stringify(obj);
    ws.send(payload);
    logger.info(`ws >> ${payload.slice(0, 300)}`);
  }
}

function startKeepAlive() {
  stopKeepAlive();
  keepAliveTimer = setInterval(() => {
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    send({ jsonrpc: '2.0', id: nextPingId++, method: 'ping', params: {} });
  }, KEEPALIVE_MS);
}

function stopKeepAlive() {
  if (keepAliveTimer) clearInterval(keepAliveTimer);
  keepAliveTimer = null;
}

function scheduleReconnect(reason: string) {
  if (reconnectTimer || shuttingDown || !XIAOZHI_WS_URL) return;
  logger.warn(`Reconnecting later: ${reason}`);
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    startWs().catch((err) => logger.error({ err }, 'WS restart failed'));
  }, RECONNECT_MS);
}

async function createJob(workflowName: string, text: string, userId?: string, correlationId?: string) {
  const response = await client.post('/v1/jobs', {
    workflowName,
    inputParams: { original_text: text },
    correlationId: correlationId || `xiaozhi_${Date.now()}`,
    source: 'xiaozhi',
    userId,
  });

  if (response.status >= 400) {
    throw new Error(typeof response.data === 'string' ? response.data : JSON.stringify(response.data));
  }

  return response.data as { job_id: string; status: string; deduplicated?: boolean };
}

async function getLatestResult(workflowName?: string) {
  const query = workflowName ? `?workflowName=${encodeURIComponent(workflowName)}` : '';
  const response = await client.get(`/v1/jobs/latest${query}`);
  if (response.status >= 400) {
    throw new Error(typeof response.data === 'string' ? response.data : JSON.stringify(response.data));
  }
  return response.data;
}

async function getJob(jobId: string) {
  const response = await client.get(`/v1/jobs/${jobId}`);
  if (response.status >= 400) {
    throw new Error(typeof response.data === 'string' ? response.data : JSON.stringify(response.data));
  }
  return response.data;
}

function buildReadText(workflow: WorkflowConfig, job: any) {
  if (!job) {
    return `Belum ada hasil tersimpan untuk workflow ${workflow.id}.`;
  }
  if (job.status === 'COMPLETED') {
    return String(job.resultData?.summary || `Workflow ${workflow.id} sudah selesai.`);
  }
  if (job.status === 'FAILED' || job.status === 'TIMEOUT') {
    const lastError = job.events?.find((e: any) => e.eventType === 'ERROR')?.message;
    return `Workflow ${workflow.id} status ${job.status}. ${lastError || ''}`.trim();
  }
  return `Workflow ${workflow.id} masih ${String(job.status).toLowerCase()}. Ref: ${job.id}.`;
}

async function handleToolCall(msg: any) {
  const toolMeta = workflowToolMap.get(msg.params?.name);

  if (toolMeta?.kind === 'run') {
    const workflow = toolMeta.workflow;
    const rawText = String(msg.params?.arguments?.text ?? msg.params?.arguments?.pesan ?? '').trim();
    if (!rawText) {
      send({ jsonrpc: '2.0', id: msg.id, error: { code: -32602, message: 'text required' } });
      return;
    }

    const result = await createJob(workflow.id, rawText);
    rememberJob(workflow.id, result.job_id);
    const resultText = `Workflow ${workflow.id} diproses. Ref: ${String(result.job_id).slice(0, 8)}. Kalau mau cek lagi, minta ${workflow.readToolName || 'get_last_result'}.`;
    send({
      jsonrpc: '2.0',
      id: msg.id,
      result: {
        content: [{ type: 'text', text: resultText }],
        structuredContent: {
          ok: true,
          workflow: workflow.id,
          queued: true,
          ref: result.job_id,
          text: resultText,
        },
      },
    });
    return;
  }

  if (toolMeta?.kind === 'read') {
    const workflow = toolMeta.workflow;
    let job: any = null;

    try {
      const latest = await getLatestResult(workflow.id);
      job = await getJob(latest.id);
    } catch {
      const rememberedJobId = getRememberedJobId(workflow.id);
      if (rememberedJobId) {
        try {
          job = await getJob(rememberedJobId);
        } catch {
          job = null;
        }
      }
    }

    const resultText = buildReadText(workflow, job);
    send({
      jsonrpc: '2.0',
      id: msg.id,
      result: {
        content: [{ type: 'text', text: resultText }],
        structuredContent: {
          ok: true,
          workflow: workflow.id,
          hasResult: Boolean(job),
          jobId: job?.id || null,
          text: resultText,
        },
      },
    });
    return;
  }

  const name = String(msg.params?.name || '');

  if (name === 'start_workflow') {
    const rawText = String(msg.params?.arguments?.text ?? '').trim();
    const workflowName = String(msg.params?.arguments?.workflow || DEFAULT_WORKFLOW).trim();
    if (!rawText) {
      send({ jsonrpc: '2.0', id: msg.id, error: { code: -32602, message: 'text required' } });
      return;
    }
    const result = await createJob(workflowName, rawText);
    rememberJob(workflowName, result.job_id);
    const text = `Tugas telah diterima. Ref: ${String(result.job_id).slice(0, 8)}. Hasil final paling aman dikirim ke Telegram.`;
    send({
      jsonrpc: '2.0',
      id: msg.id,
      result: {
        content: [{ type: 'text', text }],
        structuredContent: { ok: true, workflow: workflowName, ref: result.job_id, text },
      },
    });
    return;
  }

  if (name === 'get_last_result') {
    const latest = await getLatestResult();
    const text = `Tugas ${latest.workflowName} sudah selesai. Hasilnya: ${latest.summary}`;
    send({
      jsonrpc: '2.0',
      id: msg.id,
      result: {
        content: [{ type: 'text', text }],
        structuredContent: { ok: true, jobId: latest.id, text },
      },
    });
    return;
  }

  send({ jsonrpc: '2.0', id: msg.id, error: { code: -32601, message: `Tool not supported: ${name}` } });
}

async function handleWsMessage(msg: any) {
  if (msg.method === 'initialize') {
    send({
      jsonrpc: '2.0',
      id: msg.id,
      result: {
        protocolVersion: '2024-11-05',
        capabilities: { tools: {} },
        serverInfo: { name: 'XiaoMCP Bridge', version: '1.0.0' },
      },
    });
    return;
  }

  if (msg.method === 'ping') {
    send({ jsonrpc: '2.0', id: msg.id, result: {} });
    return;
  }

  if (msg.method === 'notifications/initialized') return;

  if (msg.method === 'tools/list') {
    send({ jsonrpc: '2.0', id: msg.id, result: { tools: customTools } });
    return;
  }

  if (msg.method === 'tools/call') {
    toolCallQueue = toolCallQueue.then(() => handleToolCall(msg)).catch((err) => {
      send({ jsonrpc: '2.0', id: msg.id, error: { code: -32000, message: String(err?.message || err) } });
    });
    return;
  }

  send({ jsonrpc: '2.0', id: msg.id, error: { code: -32601, message: `Method not supported: ${msg.method}` } });
}

async function startWs() {
  if (!XIAOZHI_WS_URL) {
    logger.info('XIAOZHI_WS_URL not set; websocket bridge disabled, HTTP bridge still available.');
    return;
  }

  logger.info(`connecting websocket ${XIAOZHI_WS_URL.slice(0, 80)}`);
  ws = new WebSocket(XIAOZHI_WS_URL);

  ws.on('open', () => {
    logger.info('websocket connected');
    startKeepAlive();
  });

  ws.on('message', async (data: WebSocket.RawData) => {
    const text = data.toString('utf8');
    logger.info(`ws << ${text.slice(0, 300)}`);
    try {
      const parsed = JSON.parse(text);
      await handleWsMessage(parsed);
    } catch (err) {
      logger.warn({ err }, 'Failed to handle websocket message');
    }
  });

  ws.on('error', (err: Error) => {
    logger.error({ err }, 'websocket error');
  });

  ws.on('close', (code: number, reason: Buffer) => {
    stopKeepAlive();
    ws = null;
    logger.warn(`websocket closed: ${code} ${reason.toString()}`);
    scheduleReconnect('websocket closed');
  });
}

fastify.get('/healthz', async () => ({ ok: true, websocket: Boolean(ws && ws.readyState === WebSocket.OPEN) }));
fastify.get('/tools', async () => ({ tools: customTools }));
fastify.post('/reload-workflows', async () => {
  loadWorkflowConfigs();
  return { ok: true, count: workflowConfigs.length };
});

fastify.post('/invoke/start_workflow', async (request, reply) => {
  try {
    const body = StartSchema.parse(request.body);
    const workflow = body.workflow || DEFAULT_WORKFLOW;
    const result = await createJob(workflow, body.text, body.userId, body.correlationId);
    rememberJob(workflow, result.job_id);

    return {
      status: 'success',
      message: 'Tugas telah diterima. Hasil final paling aman dikirim ke Telegram.',
      workflow,
      job_id: result.job_id,
      job_status: result.status,
      deduplicated: result.deduplicated,
    };
  } catch (error) {
    logger.error({ err: error }, 'Failed to create job');
    return reply.status(502).send({ status: 'error', message: 'Gagal menghubungi orchestrator.' });
  }
});

fastify.get('/invoke/get_last_result', async (request, reply) => {
  try {
    const workflowName = typeof (request.query as any)?.workflowName === 'string' ? (request.query as any).workflowName : undefined;
    const latest = await getLatestResult(workflowName);
    return {
      status: 'success',
      message: `Tugas ${latest.workflowName} sudah selesai. Hasilnya: ${latest.summary}`,
      job_id: latest.id,
    };
  } catch (_error) {
    return reply.status(404).send({ status: 'error', message: 'Belum ada hasil tugas terbaru.' });
  }
});

fastify.get('/invoke/job/:id', async (request, reply) => {
  try {
    const { id } = request.params as { id: string };
    return await getJob(id);
  } catch (_error) {
    return reply.status(404).send({ status: 'error', message: 'Job tidak ditemukan.' });
  }
});

async function shutdown() {
  shuttingDown = true;
  if (reconnectTimer) clearTimeout(reconnectTimer);
  stopKeepAlive();
  if (ws && ws.readyState === WebSocket.OPEN) ws.close();
  await fastify.close();
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

const start = async () => {
  try {
    await fastify.listen({ port: PORT, host: HOST });
    await startWs();
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

start();
