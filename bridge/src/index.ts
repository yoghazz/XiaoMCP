import fs from 'node:fs';
import path from 'node:path';
import { spawn, exec as execChild } from 'node:child_process';
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
const ADMIN_USERNAME = process.env.ADMIN_USERNAME || '';
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || '';

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
  cmd?: string;
  argsTemplate?: string;
};

const WorkflowConfigSchema = z.object({
  id: z.string().min(1),
  toolName: z.string().min(1),
  readToolName: z.string().min(1).optional().or(z.literal('')),
  description: z.string().optional(),
  readDescription: z.string().optional(),
  aliases: z.array(z.string()).optional(),
  readAliases: z.array(z.string()).optional(),
  cmd: z.string().optional(),
  argsTemplate: z.string().optional(),
});

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

function normalizeWorkflowConfig(input: z.infer<typeof WorkflowConfigSchema>): WorkflowConfig {
  return {
    id: input.id.trim(),
    toolName: input.toolName.trim(),
    readToolName: input.readToolName?.trim() || undefined,
    description: input.description?.trim() || undefined,
    readDescription: input.readDescription?.trim() || undefined,
    aliases: (input.aliases || []).map((x) => x.trim()).filter(Boolean),
    readAliases: (input.readAliases || []).map((x) => x.trim()).filter(Boolean),
    cmd: input.cmd?.trim() || undefined,
    argsTemplate: input.argsTemplate?.trim() || undefined,
  };
}

function saveWorkflowConfigs() {
  ensureParentDir(WORKFLOWS_CONFIG_PATH);
  fs.writeFileSync(WORKFLOWS_CONFIG_PATH, JSON.stringify(workflowConfigs, null, 2));
  const routerPath = '/home/yoga/.openclaw/workspace/XiaoMCP/worker/workflow-router.json';
  const router: Record<string, { cmd?: string; argsTemplate?: string }> = {};
  for (const wf of workflowConfigs) {
    if (wf.cmd || wf.argsTemplate) {
      router[wf.id] = { cmd: wf.cmd, argsTemplate: wf.argsTemplate };
    }
  }
  fs.writeFileSync(routerPath, JSON.stringify(router, null, 2));
  rebuildWorkflowMaps();
}

function loadWorkflowConfigs() {
  try {
    if (!fs.existsSync(WORKFLOWS_CONFIG_PATH)) {
      workflowConfigs = [];
      rebuildWorkflowMaps();
      return;
    }
    const parsed = JSON.parse(fs.readFileSync(WORKFLOWS_CONFIG_PATH, 'utf8'));
    workflowConfigs = Array.isArray(parsed)
      ? parsed.filter(Boolean).map((item) => normalizeWorkflowConfig(WorkflowConfigSchema.parse(item)))
      : [];
    rebuildWorkflowMaps();
  } catch (err) {
    logger.warn({ err }, 'Failed to load workflow config');
    workflowConfigs = [];
    rebuildWorkflowMaps();
  }
}

loadWorkflowConfigs();

function unauthorizedAdmin(reply: any) {
  reply.header('WWW-Authenticate', 'Basic realm="XiaoMCP Admin"');
  return reply.status(401).send('Unauthorized');
}

function verifyAdminAuth(request: any, reply: any) {
  if (!ADMIN_USERNAME || !ADMIN_PASSWORD) {
    return reply.status(403).send({ message: 'Admin UI disabled. Set ADMIN_USERNAME and ADMIN_PASSWORD first.' });
  }

  const header = String(request.headers.authorization || '');
  if (!header.startsWith('Basic ')) {
    return unauthorizedAdmin(reply);
  }

  try {
    const decoded = Buffer.from(header.slice(6), 'base64').toString('utf8');
    const sep = decoded.indexOf(':');
    const username = sep >= 0 ? decoded.slice(0, sep) : decoded;
    const password = sep >= 0 ? decoded.slice(sep + 1) : '';
    if (username !== ADMIN_USERNAME || password !== ADMIN_PASSWORD) {
      return unauthorizedAdmin(reply);
    }
  } catch {
    return unauthorizedAdmin(reply);
  }
}

function renderAdminPage() {
  const initialTools = JSON.stringify(workflowConfigs);
  const initialSettings = JSON.stringify({
    orchestratorUrl: API_URL,
    defaultWorkflow: DEFAULT_WORKFLOW,
    workflowsConfigPath: WORKFLOWS_CONFIG_PATH,
    bridgeStatePath: STATE_PATH,
    host: HOST,
    port: PORT,
    keepaliveMs: KEEPALIVE_MS,
    reconnectMs: RECONNECT_MS,
    xiaozhiWsUrl: XIAOZHI_WS_URL,
    websocketActive: Boolean(ws && ws.readyState === WebSocket.OPEN),
    adminAuthEnabled: Boolean(ADMIN_USERNAME && ADMIN_PASSWORD),
    workflowCount: workflowConfigs.length,
  });
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>XiaoMCP Admin</title>
  <style>
    body{font-family:system-ui,sans-serif;max-width:1180px;margin:32px auto;padding:0 16px;background:#0b1020;color:#e8ecf3}
    .card{background:#141a2e;border:1px solid #27304d;border-radius:16px;padding:18px}
    .wrap{display:grid;grid-template-columns:1.1fr 0.9fr;gap:20px}
    .tabs{display:flex;gap:10px;flex-wrap:wrap;margin-top:18px}
    .tab-btn{background:#24304f}.tab-btn.active{background:#6d7cff}
    .tab{display:none;margin-top:18px}.tab.active{display:block}
    input,textarea{width:100%;box-sizing:border-box;background:#0e1426;color:#fff;border:1px solid #334064;border-radius:10px;padding:10px;margin-top:6px}
    textarea{min-height:84px} button{background:#6d7cff;color:#fff;border:0;border-radius:10px;padding:10px 14px;cursor:pointer}
    button.alt{background:#24304f} button.danger{background:#b94b63} .row{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    .item{border:1px solid #27304d;border-radius:12px;padding:12px;margin-bottom:10px}.muted{color:#9aa7c2;font-size:13px}
    .actions{display:flex;gap:8px;flex-wrap:wrap}.top{display:flex;justify-content:space-between;align-items:center;gap:12px}
    .kv{display:grid;grid-template-columns:220px 1fr;gap:10px;padding:10px 0;border-bottom:1px solid #27304d}.mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;word-break:break-all}
    @media(max-width:900px){.wrap,.row,.kv{grid-template-columns:1fr}}
  </style>
</head>
<body>
  <div class="top"><div><h1 style="margin:0">XiaoMCP Admin</h1><div class="muted">Kelola workflow dan setting untuk Xiaozhi bridge.</div></div><button class="alt" type="button" onclick="refreshActiveTab()">Refresh</button></div>
  <div class="tabs">
    <button class="tab-btn active" type="button" data-tab="workflow" onclick="switchTab('workflow')">Workflow</button>
    <button class="tab-btn" type="button" data-tab="router" onclick="switchTab('router')">Router</button>
    <button class="tab-btn" type="button" data-tab="setting" onclick="switchTab('setting')">Setting</button>
  </div>
  <section id="tab-workflow" class="tab active">
    <div class="wrap" style="margin-top:20px">
      <div class="card">
        <h3>Daftar workflow tool</h3>
        <div id="toolList"></div>
      </div>
      <div class="card">
        <h3 id="formTitle">Buat workflow tool baru</h3>
        <form id="toolForm">
          <input type="hidden" id="originalId" />
          <label>Workflow ID<input id="id" required placeholder="kopi_instagram" /></label>
          <label>Tool name<input id="toolName" required placeholder="kopi_instagram" /></label>
          <label>Read tool name<input id="readToolName" placeholder="baca_hasil_kopi_instagram" /></label>
          <div class="row">
            <label>Aliases (pisahkan koma)<input id="aliases" placeholder="buat_kopi_instagram" /></label>
            <label>Read aliases (pisahkan koma)<input id="readAliases" placeholder="cek_kopi_instagram" /></label>
          </div>
          <label>Deskripsi run<textarea id="description"></textarea></label>
          <label>Deskripsi read<textarea id="readDescription"></textarea></label>
          <label>Command<input id="cmd" placeholder="openclaw" /></label>
          <label>Args template<textarea id="argsTemplate" placeholder="agent --json --session-id {{activeSessionId}} --message &quot;{{text}}&quot;"></textarea></label>
          <div class="muted" style="margin-top:6px">Variabel tersedia: <code>{{text}}</code>, <code>{{activeSessionId}}</code></div>
          <div class="actions"><button type="submit">Simpan</button><button type="button" class="alt" onclick="resetForm()">Reset</button></div>
        </form>
        <p class="muted" id="status"></p>
      </div>
    </div>
  </section>
  <section id="tab-router" class="tab">
    <div class="card" style="margin-top:20px">
      <h3>Workflow Router</h3>
      <div class="muted">Atur command dan args template per workflow untuk worker XiaoMCP.</div>
      <textarea id="routerJson" style="min-height:320px;margin-top:12px;font-family:ui-monospace,SFMono-Regular,Menlo,monospace"></textarea>
      <div class="actions" style="margin-top:12px"><button type="button" id="saveRouterBtn">Simpan Router</button><button type="button" class="alt" id="reloadRouterBtn">Reload Router</button></div>
      <p class="muted" id="routerStatus"></p>
    </div>
  </section>
  <section id="tab-setting" class="tab">
    <div class="card" style="margin-top:20px">
      <h3>Setting bridge</h3>
      <div id="settingsBox"></div>
      <form id="settingsForm" style="margin-top:18px">
        <div class="row">
          <label>Orchestrator URL<input id="s_orchestratorUrl" /></label>
          <label>Default workflow<input id="s_defaultWorkflow" /></label>
        </div>
        <div class="row">
          <label>Host<input id="s_host" /></label>
          <label>Port<input id="s_port" type="number" /></label>
        </div>
        <div class="row">
          <label>Keepalive ms<input id="s_keepaliveMs" type="number" /></label>
          <label>Reconnect ms<input id="s_reconnectMs" type="number" /></label>
        </div>
        <label>Xiaozhi WS URL<input id="s_xiaozhiWsUrl" /></label>
        <div class="actions" style="margin-top:12px"><button type="submit">Simpan Setting</button><button type="button" class="alt" id="restartBridgeBtn">Restart Bridge</button></div>
      </form>
      <p class="muted" id="settingsStatus"></p>
    </div>
  </section>
<script>
  const $ = (id) => document.getElementById(id);
  const splitCsv = (v) => v.split(',').map(x => x.trim()).filter(Boolean);
  let tools = ${initialTools};
  let currentTab = 'workflow';
  const initialSettings = ${initialSettings};
  function esc(s){return String(s||'').replace(/[&<>"']/g,function(c){ if(c==='&') return '&amp;'; if(c==='<') return '&lt;'; if(c==='>') return '&gt;'; if(c==='\"') return '&quot;'; return '&#39;'; })}
  function setStatus(msg){$('status').textContent = msg || ''}
  function setSettingsStatus(msg){$('settingsStatus').textContent = msg || ''}
  function setRouterStatus(msg){$('routerStatus').textContent = msg || ''}
  function resetForm(){ $('formTitle').textContent='Buat workflow tool baru'; $('originalId').value=''; $('toolForm').reset(); setStatus(''); }
  function switchTab(name){ currentTab=name; document.querySelectorAll('.tab').forEach(el=>el.classList.remove('active')); document.querySelectorAll('.tab-btn').forEach(el=>el.classList.remove('active')); $('tab-'+name).classList.add('active'); document.querySelector('[data-tab="'+name+'"]').classList.add('active'); refreshActiveTab(); }
  function refreshActiveTab(){ if(currentTab==='workflow') loadTools(); else if(currentTab==='router') loadRouter(); else loadSettings(); }
  function editTool(id){ const t = tools.find(x => x.id===id); if(!t) return; switchTab('workflow'); $('formTitle').textContent='Edit workflow tool'; $('originalId').value=t.id; $('id').value=t.id; $('toolName').value=t.toolName||''; $('readToolName').value=t.readToolName||''; $('aliases').value=(t.aliases||[]).join(', '); $('readAliases').value=(t.readAliases||[]).join(', '); $('description').value=t.description||''; $('readDescription').value=t.readDescription||''; $('cmd').value=t.cmd||''; $('argsTemplate').value=t.argsTemplate||''; window.scrollTo({top:0,behavior:'smooth'}); }
  async function deleteTool(id){ if(!confirm('Hapus tool '+id+'?')) return; const r = await fetch('/admin/api/tools/'+encodeURIComponent(id), {method:'DELETE',headers:{'cache-control':'no-cache'}}); const j = await r.json(); setStatus(j.message || (r.ok ? 'Terhapus' : 'Gagal')); if(r.ok) loadTools(); }
  function renderList(){ if(!tools.length){ $('toolList').innerHTML='<div class="muted">Belum ada workflow tool.</div>'; return; } $('toolList').innerHTML = tools.map(function(t){ var readPart = t.readToolName ? ' · read: ' + esc(t.readToolName) : ''; return '<div class="item"><div style="display:flex;justify-content:space-between;gap:12px"><div><b>' + esc(t.id) + '</b><div class="muted">run: ' + esc(t.toolName) + readPart + '</div></div><div class="actions"><button class="alt" type="button" data-edit="' + esc(t.id) + '">Edit</button><button class="danger" type="button" data-del="' + esc(t.id) + '">Hapus</button></div></div><div class="muted" style="margin-top:8px">aliases: ' + esc((t.aliases||[]).join(', ')||'-') + '</div><div class="muted">read aliases: ' + esc((t.readAliases||[]).join(', ')||'-') + '</div></div>'; }).join(''); document.querySelectorAll('[data-edit]').forEach(el=>el.onclick=()=>editTool(el.getAttribute('data-edit'))); document.querySelectorAll('[data-del]').forEach(el=>el.onclick=()=>deleteTool(el.getAttribute('data-del'))); }
  function renderSettings(data){ const rows = [['Orchestrator URL', data.orchestratorUrl],['Default workflow', data.defaultWorkflow],['Workflows config path', data.workflowsConfigPath],['Bridge state path', data.bridgeStatePath],['Host', data.host],['Port', data.port],['Keepalive ms', data.keepaliveMs],['Reconnect ms', data.reconnectMs],['WebSocket Xiaozhi', data.xiaozhiWsUrl || '-'],['WebSocket active', data.websocketActive ? 'yes' : 'no'],['Admin auth', data.adminAuthEnabled ? 'enabled' : 'disabled'],['Workflow count', String(data.workflowCount ?? 0)]]; $('settingsBox').innerHTML = rows.map(function(r){ return '<div class="kv"><div class="muted">'+esc(r[0])+'</div><div class="mono">'+esc(String(r[1]))+'</div></div>'; }).join(''); $('s_orchestratorUrl').value=data.orchestratorUrl||''; $('s_defaultWorkflow').value=data.defaultWorkflow||''; $('s_host').value=data.host||''; $('s_port').value=data.port||''; $('s_keepaliveMs').value=data.keepaliveMs||''; $('s_reconnectMs').value=data.reconnectMs||''; $('s_xiaozhiWsUrl').value=data.xiaozhiWsUrl||''; }
  async function loadTools(){ const r = await fetch('/admin/api/tools',{headers:{'cache-control':'no-cache'}}); const j = await r.json(); tools = j.tools || []; renderList(); }
  async function loadSettings(){ const r = await fetch('/admin/api/settings',{headers:{'cache-control':'no-cache'}}); const j = await r.json(); if(!r.ok){ setSettingsStatus(j.message || 'Gagal load setting'); return; } setSettingsStatus(''); renderSettings(j); }
  async function loadRouter(){ const r = await fetch('/admin/api/router',{headers:{'cache-control':'no-cache'}}); const j = await r.json(); if(!r.ok){ setRouterStatus(j.message || 'Gagal load router'); return; } $('routerJson').value = JSON.stringify(j.router || {}, null, 2); setRouterStatus(''); }
  $('toolForm').addEventListener('submit', async (e) => { e.preventDefault(); const originalId = $('originalId').value.trim(); const payload = { id:$('id').value.trim(), toolName:$('toolName').value.trim(), readToolName:$('readToolName').value.trim(), aliases:splitCsv($('aliases').value), readAliases:splitCsv($('readAliases').value), description:$('description').value.trim(), readDescription:$('readDescription').value.trim(), cmd:$('cmd').value.trim(), argsTemplate:$('argsTemplate').value.trim() }; const method = originalId ? 'PUT' : 'POST'; const url = originalId ? '/admin/api/tools/'+encodeURIComponent(originalId) : '/admin/api/tools'; const r = await fetch(url,{method,headers:{'content-type':'application/json','cache-control':'no-cache'},body:JSON.stringify(payload)}); const j = await r.json(); setStatus(j.message || (r.ok ? 'Tersimpan' : 'Gagal')); if(r.ok){ resetForm(); loadTools(); } });
  $('settingsForm').addEventListener('submit', async (e) => { e.preventDefault(); const payload = { orchestratorUrl:$('s_orchestratorUrl').value.trim(), defaultWorkflow:$('s_defaultWorkflow').value.trim(), host:$('s_host').value.trim(), port:Number($('s_port').value||0), keepaliveMs:Number($('s_keepaliveMs').value||0), reconnectMs:Number($('s_reconnectMs').value||0), xiaozhiWsUrl:$('s_xiaozhiWsUrl').value.trim() }; const r = await fetch('/admin/api/settings',{method:'PUT',headers:{'content-type':'application/json','cache-control':'no-cache'},body:JSON.stringify(payload)}); const j = await r.json(); setSettingsStatus(j.message || (r.ok ? 'Tersimpan' : 'Gagal')); if(r.ok) loadSettings(); });
  $('restartBridgeBtn').addEventListener('click', async () => { const ok = confirm('Restart bridge sekarang?'); if(!ok) return; const r = await fetch('/admin/api/restart-bridge',{method:'POST',headers:{'content-type':'application/json','cache-control':'no-cache'},body:'{}'}); const j = await r.json(); setSettingsStatus(j.message || (r.ok ? 'Restart diminta' : 'Gagal restart')); });
  $('saveRouterBtn').addEventListener('click', async () => { let payload; try { payload = JSON.parse($('routerJson').value || '{}'); } catch(e) { setRouterStatus('JSON router tidak valid'); return; } const r = await fetch('/admin/api/router',{method:'PUT',headers:{'content-type':'application/json','cache-control':'no-cache'},body:JSON.stringify({router: payload})}); const j = await r.json(); setRouterStatus(j.message || (r.ok ? 'Router tersimpan' : 'Gagal simpan router')); });
  $('reloadRouterBtn').addEventListener('click', async () => { await loadRouter(); setRouterStatus('Router direload'); });
  renderList(); renderSettings(initialSettings); loadRouter();
</script>
</body></html>`;
}

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

fastify.addHook('preHandler', async (request, reply) => {
  if (request.url.startsWith('/admin/')) {
    const result = verifyAdminAuth(request, reply);
    if (result) return result;
  }
});

fastify.get('/healthz', async () => ({ ok: true, websocket: Boolean(ws && ws.readyState === WebSocket.OPEN) }));
fastify.get('/tools', async () => ({ tools: customTools }));
fastify.get('/admin/tools', async (_request, reply) => {
  reply.header('content-type', 'text/html; charset=utf-8');
  return renderAdminPage();
});
fastify.get('/admin/api/tools', async () => ({ tools: workflowConfigs }));
fastify.get('/admin/api/settings', async () => ({
  orchestratorUrl: API_URL,
  defaultWorkflow: DEFAULT_WORKFLOW,
  workflowsConfigPath: WORKFLOWS_CONFIG_PATH,
  bridgeStatePath: STATE_PATH,
  host: HOST,
  port: PORT,
  keepaliveMs: KEEPALIVE_MS,
  reconnectMs: RECONNECT_MS,
  xiaozhiWsUrl: XIAOZHI_WS_URL,
  websocketActive: Boolean(ws && ws.readyState === WebSocket.OPEN),
  adminAuthEnabled: Boolean(ADMIN_USERNAME && ADMIN_PASSWORD),
  workflowCount: workflowConfigs.length,
}));
fastify.put('/admin/api/settings', async (request, reply) => {
  try {
    const body = request.body as any;
    const envPath = path.resolve(process.cwd(), '.env');
    let envText = fs.existsSync(envPath) ? fs.readFileSync(envPath, 'utf8') : '';
    const updates: Record<string, string> = {
      ORCHESTRATOR_URL: String(body.orchestratorUrl || ''),
      DEFAULT_WORKFLOW: String(body.defaultWorkflow || ''),
      HOST: String(body.host || ''),
      PORT: String(body.port || ''),
      KEEPALIVE_MS: String(body.keepaliveMs || ''),
      RECONNECT_MS: String(body.reconnectMs || ''),
      XIAOZHI_WS_URL: String(body.xiaozhiWsUrl || ''),
    };
    for (const [key, value] of Object.entries(updates)) {
      const re = new RegExp(`^${key}=.*$`, 'm');
      if (re.test(envText)) envText = envText.replace(re, `${key}=${value}`);
      else envText += `${envText.endsWith('\n') || !envText ? '' : '\n'}${key}=${value}\n`;
    }
    fs.writeFileSync(envPath, envText);
    return { ok: true, message: 'Setting berhasil disimpan. Restart bridge untuk menerapkan perubahan env.' };
  } catch (err) {
    return reply.status(400).send({ message: String((err as Error).message || err) });
  }
});
fastify.post('/admin/api/tools', async (request, reply) => {
  try {
    const parsed = normalizeWorkflowConfig(WorkflowConfigSchema.parse(request.body));
    if (workflowConfigs.some((item) => item.id === parsed.id)) {
      return reply.status(409).send({ message: 'Workflow ID sudah ada.' });
    }
    workflowConfigs.push(parsed);
    saveWorkflowConfigs();
    return { ok: true, message: 'Tool berhasil dibuat.', tool: parsed };
  } catch (err) {
    return reply.status(400).send({ message: String((err as Error).message || err) });
  }
});
fastify.put('/admin/api/tools/:id', async (request, reply) => {
  try {
    const { id } = request.params as { id: string };
    const idx = workflowConfigs.findIndex((item) => item.id === id);
    if (idx === -1) return reply.status(404).send({ message: 'Tool tidak ditemukan.' });
    const parsed = normalizeWorkflowConfig(WorkflowConfigSchema.parse(request.body));
    if (parsed.id !== id && workflowConfigs.some((item) => item.id === parsed.id)) {
      return reply.status(409).send({ message: 'Workflow ID baru sudah dipakai.' });
    }
    workflowConfigs[idx] = parsed;
    saveWorkflowConfigs();
    return { ok: true, message: 'Tool berhasil diupdate.', tool: parsed };
  } catch (err) {
    return reply.status(400).send({ message: String((err as Error).message || err) });
  }
});
fastify.delete('/admin/api/tools/:id', async (request, reply) => {
  const { id } = request.params as { id: string };
  const before = workflowConfigs.length;
  workflowConfigs = workflowConfigs.filter((item) => item.id !== id);
  if (workflowConfigs.length === before) {
    return reply.status(404).send({ message: 'Tool tidak ditemukan.' });
  }
  saveWorkflowConfigs();
  return { ok: true, message: 'Tool berhasil dihapus.' };
});
fastify.post('/reload-workflows', async () => {
  loadWorkflowConfigs();
  return { ok: true, count: workflowConfigs.length };
});
fastify.get('/admin/api/router', async (_request, reply) => {
  try {
    const routerPath = '/home/yoga/.openclaw/workspace/XiaoMCP/worker/workflow-router.json';
    const router = fs.existsSync(routerPath) ? JSON.parse(fs.readFileSync(routerPath, 'utf8')) : {};
    return { router };
  } catch (err) {
    return reply.status(400).send({ message: String((err as Error).message || err) });
  }
});
fastify.put('/admin/api/router', async (request, reply) => {
  try {
    const routerPath = '/home/yoga/.openclaw/workspace/XiaoMCP/worker/workflow-router.json';
    const body = request.body as any;
    fs.writeFileSync(routerPath, JSON.stringify(body?.router || {}, null, 2));
    return { ok: true, message: 'Router berhasil disimpan.' };
  } catch (err) {
    return reply.status(400).send({ message: String((err as Error).message || err) });
  }
});
fastify.post('/admin/api/restart-bridge', async (_request, reply) => {
  try {
    const cmd = `cd ${JSON.stringify(process.cwd())} && nohup bash -lc 'sleep 2; exec ${process.execPath} dist/index.js' >/tmp/xiaomcp-bridge.log 2>&1 &`;
    execChild(cmd);
    setTimeout(() => {
      process.exit(0);
    }, 500);
    return { ok: true, message: 'Restart bridge diminta. Tunggu beberapa detik lalu refresh halaman.' };
  } catch (err) {
    return reply.status(500).send({ message: String((err as Error).message || err) });
  }
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
