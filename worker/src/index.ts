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
const API_KEY = process.env.API_KEY || 'default-secret';
const WORKER_NAME = process.env.WORKER_NAME || 'local-worker';

const client = axios.create({
  baseURL: API_URL,
  headers: { 'x-api-key': API_KEY }
});

async function pollForJobs() {
  logger.info('Polling for jobs...');
  try {
    const response = await client.get('/v1/worker/next-job');
    
    if (response.status === 204) {
      return; // No jobs
    }

    const job = response.data;
    logger.info(`Found job: ${job.id} (${job.workflowName})`);

    // 1. Mark as running
    await client.patch(`/v1/jobs/${job.id}`, {
      status: 'RUNNING',
      event: { type: 'PROGRESS', message: 'Starting OpenClaw task...' }
    });

    // 2. Execute OpenClaw
    logger.info(`Executing workflow: ${job.workflowName}`);
    
    try {
      // Misal: openclaw run --workflow research --input "..."
      const command = `echo "Simulating OpenClaw run for ${job.workflowName}"`; 
      const { stdout, stderr } = await execAsync(command);
      
      if (stderr) logger.warn(`OpenClaw stderr: ${stderr}`);

      // 3. Mark as completed
      await client.patch(`/v1/jobs/${job.id}`, {
        status: 'COMPLETED',
        resultData: { 
          summary: stdout.trim() || `Workflow ${job.workflowName} executed.`,
          timestamp: new Date().toISOString()
        },
        event: { type: 'PROGRESS', message: 'Task finished successfully.' }
      });
    } catch (execError) {
      logger.error(`Execution error: ${(execError as Error).message}`);
      await client.patch(`/v1/jobs/${job.id}`, {
        status: 'FAILED',
        event: { type: 'ERROR', message: `Execution failed: ${(execError as Error).message}` }
      });
    }

    logger.info(`Job ${job.id} completed.`);
  } catch (error) {
    logger.error(`Error polling jobs: ${(error as Error).message}`);
  }
}

async function start() {
  logger.info(`Worker ${WORKER_NAME} starting...`);
  
  // Heartbeat loop
  setInterval(async () => {
    try {
      await client.post('/v1/worker/heartbeat', { name: WORKER_NAME });
    } catch (e) {}
  }, 30000);

  // Job loop
  while (true) {
    await pollForJobs();
    await new Promise(resolve => setTimeout(resolve, 5000)); // Poll every 5s
  }
}

start();
