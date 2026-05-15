import axios from 'axios';
import pino from 'pino';
import dotenv from 'dotenv';

dotenv.config();

const logger = pino({
  transport: {
    target: 'pino-pretty',
    options: { colorize: true }
  }
});

const API_URL = process.env.ORCHESTRATOR_URL || 'http://localhost:3000';
const API_KEY = process.env.API_KEY || 'default-secret';

const client = axios.create({
  baseURL: API_URL,
  headers: { 'x-api-key': API_KEY }
});

// Simulate receiving a request from Xiaozhi
async function startWorkflow(workflow: string, text: string) {
  logger.info(`Starting workflow ${workflow} for user request: ${text}`);
  
  try {
    const response = await client.post('/v1/jobs', {
      workflowName: workflow,
      inputParams: { original_text: text },
      correlationId: `xiaozhi_${Date.now()}`
    });

    const { job_id } = response.data;
    logger.info(`Job created successfully. ID: ${job_id}`);
    
    return {
      status: 'success',
      message: 'Tugas telah diterima. Hasil akan dikirim ke Telegram setelah selesai.',
      job_id
    };
  } catch (error) {
    logger.error(`Failed to create job: ${(error as Error).message}`);
    return { status: 'error', message: 'Gagal menghubungi server pusat.' };
  }
}

// --- TOOL DEFINITIONS FOR XIAOZHI ---
const tools = [
  {
    name: 'start_workflow',
    description: 'Menjalankan workflow OpenClaw (misal: riset, otomasi, dll)',
    parameters: { type: 'object', properties: { workflow: { type: 'string' }, text: { type: 'string' } } }
  },
  {
    name: 'get_last_result',
    description: 'Mengecek hasil dari tugas terakhir yang sudah selesai',
    parameters: { type: 'object', properties: {} }
  }
];

async function getLastResult() {
  try {
    const response = await client.get('/v1/jobs/latest');
    const { workflowName, summary } = response.data;
    return `Tugas ${workflowName} sudah selesai. Hasilnya adalah: ${summary}`;
  } catch (error) {
    return 'Maaf, saya tidak menemukan hasil tugas terbaru.';
  }
}

// --- PROACTIVE NOTIFICATION (Simulasi) ---
function enableProactiveMode() {
  logger.info('Proactive notification listener enabled.');
  // Jika VPS kirim sinyal 'job_done', panggil fungsi TTS Xiaozhi di sini
}

enableProactiveMode();

// In real scenario, this would be a WebSocket server/client listening to Xiaozhi
logger.info('Xiaozhi Bridge initialized.');

// Example trigger
setTimeout(() => {
  startWorkflow('openclaw_research', 'Bantu riset tentang arsitektur microservices.');
}, 2000);
