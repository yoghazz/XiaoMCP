import Fastify from 'fastify';
import { jobService } from './services/JobService';
import { z } from 'zod';
import pino from 'pino';
import dotenv from 'dotenv';

dotenv.config();

const logger = pino({
  transport: { target: 'pino-pretty' }
});

const fastify = Fastify({ logger });
const API_KEY = process.env.API_KEY || 'default-secret';
const RUNTIME_MODE = process.env.RUNTIME_MODE || 'local';

// Middleware for API Key (Only if not in embedded local mode if we decide to skip it)
fastify.addHook('preHandler', async (request, reply) => {
  // For simplicity, we still keep API Key even for local HTTP
  const apiKey = request.headers['x-api-key'];
  if (!apiKey || apiKey !== API_KEY) {
    return reply.status(401).send({ error: 'Unauthorized' });
  }
});

// --- Routes ---

fastify.post('/v1/jobs', async (request, reply) => {
  const data = request.body as any;
  const job = await jobService.createJob(data);
  return { job_id: job.id, status: job.status };
});

fastify.get('/v1/jobs/:id', async (request, reply) => {
  const { id } = request.params as { id: string };
  return await jobService.getJob(id);
});

fastify.get('/v1/jobs/latest', async (request, reply) => {
  const job = await jobService.getLatestJob();
  if (!job) return reply.status(404).send({ error: 'No jobs found' });
  return {
    id: job.id,
    workflowName: job.workflowName,
    summary: job.resultData ? JSON.parse(job.resultData).summary : 'No summary'
  };
});

fastify.post('/v1/worker/heartbeat', async (request, reply) => {
  const { workerId, name } = request.body as { workerId?: string; name: string };
  return await jobService.heartbeat(name, workerId);
});

fastify.get('/v1/worker/next-job', async (request, reply) => {
  const job = await jobService.claimNextJob();
  if (!job) return reply.status(204).send();
  return job;
});

fastify.patch('/v1/jobs/:id', async (request, reply) => {
  const { id } = request.params as { id: string };
  return await jobService.updateJob(id, request.body as any);
});

const start = async () => {
  try {
    const port = parseInt(process.env.PORT || '3000');
    await fastify.listen({ port, host: '0.0.0.0' });
    logger.info(`Orchestrator running in ${RUNTIME_MODE} mode on port ${port}`);
  } catch (err) {
    logger.error(err);
    process.exit(1);
  }
};

if (require.main === module) {
  start();
}

export { jobService };
