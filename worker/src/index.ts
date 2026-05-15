import axios from 'axios';
import pino from 'pino';
import dotenv from 'dotenv';
import { exec } from 'child_process';
import { promisify } from 'util';

const execAsync = promisify(exec);

dotenv.config();

const logger = pino({
  transport: {
    target: 'pino-pretty',
    options: { colorize: true }
  }
});

const API_URL = process.env.ORCHESTRATOR_URL || 'http://localhost:3000';
const API_KEY = process.env.API_KEY || 'change-me';
const WORKER_NAME = process.env.WORKER_NAME || 'local-worker';
const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS || 5000);
const EXECUTION_MODE = process.env.EXECUTION_MODE || 'demo';
const OPENCLAW_CMD = process.env.OPENCLAW_CMD || 'echo';
const OPENCLAW_ARGS_TEMPLATE = process.env.OPENCLAW_ARGS_TEMPLATE || 'Simulating OpenClaw run for {{workflow}}: {{text}}';

const client = axios.create({
  baseURL: API_URL,
  headers: { 'x-api-key': API_KEY },
  timeout: 30000,
  validateStatus: () => true,
});

let workerId: string | undefined;

function shellEscape(value: string) {
  return `'${value.replace(/'/g, `'\\''`)}'`;
}

function renderTemplate(template: string, vars: Record<string, string>) {
  return template.replace(/\{\{\s*(\w+)\s*\}\}/g, (_, key) => vars[key] ?? '');
}

async function sendHeartbeat() {
  const response = await client.post('/v1/worker/heartbeat', { workerId, name: WORKER_NAME });
  if (response.status >= 400) {
    throw new Error(`Heartbeat failed with status ${response.status}`);
  }
  workerId = response.data.id;
}

async function claimJob() {
  const response = await client.post('/v1/worker/claim-next', { workerId, workerName: WORKER_NAME });
  if (response.status === 204) return null;
  if (response.status >= 400) throw new Error(`Claim failed with status ${response.status}`);
  return response.data;
}

async function updateJob(jobId: string, payload: Record<string, any>) {
  const response = await client.patch(`/v1/jobs/${jobId}`, payload);
  if (response.status >= 400) {
    throw new Error(`Update failed with status ${response.status}: ${JSON.stringify(response.data)}`);
  }
}

async function executeJob(job: any) {
  const text = String(job.inputParams?.original_text ?? '');
  logger.info(`Executing job ${job.id} (${job.workflowName}) in mode=${EXECUTION_MODE}`);

  await updateJob(job.id, {
    event: { type: 'PROGRESS', message: 'Starting task execution...' }
  });

  try {
    let summary: string;

    if (EXECUTION_MODE === 'openclaw') {
      const renderedArgs = renderTemplate(OPENCLAW_ARGS_TEMPLATE, {
        workflow: String(job.workflowName),
        text,
      });
      const command = `${OPENCLAW_CMD} ${renderedArgs}`;
      const { stdout, stderr } = await execAsync(command, { maxBuffer: 5 * 1024 * 1024 });
      if (stderr?.trim()) {
        await updateJob(job.id, { event: { type: 'LOG', message: stderr.trim() } });
      }
      summary = (stdout || '').trim() || `Workflow ${job.workflowName} executed.`;
    } else {
      const safeText = text.slice(0, 500);
      const { stdout } = await execAsync(`echo ${shellEscape(`Workflow ${job.workflowName} menerima: ${safeText}`)}`);
      summary = stdout.trim();
    }

    await updateJob(job.id, {
      status: 'COMPLETED',
      resultData: {
        summary,
        workflow: job.workflowName,
        timestamp: new Date().toISOString(),
      },
      event: { type: 'PROGRESS', message: 'Task finished successfully.' }
    });
  } catch (execError) {
    logger.error({ err: execError }, `Execution error for job ${job.id}`);
    await updateJob(job.id, {
      status: 'FAILED',
      event: { type: 'ERROR', message: `Execution failed: ${(execError as Error).message}` }
    });
  }
}

async function start() {
  logger.info(`Worker ${WORKER_NAME} starting...`);

  await sendHeartbeat();
  setInterval(() => {
    sendHeartbeat().catch((err) => logger.error({ err }, 'Heartbeat failed'));
  }, 30000);

  while (true) {
    try {
      const job = await claimJob();
      if (job) {
        await executeJob(job);
      }
    } catch (error) {
      logger.error({ err: error }, 'Worker loop error');
    }
    await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL_MS));
  }
}

start().catch((err) => {
  logger.error({ err }, 'Worker failed to start');
  process.exit(1);
});
