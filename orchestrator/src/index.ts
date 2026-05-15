import Fastify from 'fastify';
import { PrismaClient } from '@prisma/client';
import { z } from 'zod';
import pino from 'pino';
import dotenv from 'dotenv';
import axios from 'axios';

dotenv.config();

const logger = pino({
  transport: {
    target: 'pino-pretty',
    options: { colorize: true }
  }
});

const fastify = Fastify({ logger });
const prisma = new PrismaClient();

const API_KEY = process.env.API_KEY || 'default-secret';

// Middleware for API Key
fastify.addHook('preHandler', async (request, reply) => {
  const apiKey = request.headers['x-api-key'];
  if (!apiKey || apiKey !== API_KEY) {
    reply.status(401).send({ error: 'Unauthorized' });
  }
});

// --- Schemas ---
const CreateJobSchema = z.object({
  workflowName: z.string(),
  inputParams: z.any(),
  correlationId: z.string().optional(),
});

const UpdateJobSchema = z.object({
  status: z.enum(['RUNNING', 'COMPLETED', 'FAILED', 'TIMEOUT']).optional(),
  resultData: z.any().optional(),
  event: z.object({
    type: z.enum(['PROGRESS', 'LOG', 'ERROR']),
    message: z.string(),
    payload: z.any().optional(),
  }).optional(),
});

// --- Routes ---

// 1. Create Job (called by Bridge)
fastify.post('/v1/jobs', async (request, reply) => {
  try {
    const data = CreateJobSchema.parse(request.body);
    const job = await prisma.job.create({
      data: {
        workflowName: data.workflowName,
        inputParams: data.inputParams,
        correlationId: data.correlationId,
        status: 'PENDING',
      },
    });
    return { job_id: job.id, status: job.status };
  } catch (err) {
    reply.status(400).send({ error: (err as Error).message });
  }
});

// 2. Get Job Status
fastify.get('/v1/jobs/:id', async (request, reply) => {
  const { id } = request.params as { id: string };
  const job = await prisma.job.findUnique({
    where: { id },
    include: { events: { orderBy: { createdAt: 'desc' }, take: 5 } }
  });
  if (!job) return reply.status(404).send({ error: 'Job not found' });
  return job;
});

// 2.5 Get Latest Completed Job (For Xiaozhi "Ask Back" mode)
fastify.get('/v1/jobs/latest', async (request, reply) => {
  const job = await prisma.job.findFirst({
    where: { status: 'COMPLETED' },
    orderBy: { updatedAt: 'desc' },
  });
  if (!job) return reply.status(404).send({ error: 'Belum ada tugas yang selesai.' });
  return {
    id: job.id,
    workflowName: job.workflowName,
    summary: (job.resultData as any)?.summary || 'Tugas selesai.'
  };
});

// 3. Worker: Heartbeat
fastify.post('/v1/worker/heartbeat', async (request, reply) => {
  const { workerId, name } = request.body as { workerId?: string; name: string };
  const worker = await prisma.worker.upsert({
    where: { id: workerId || 'new-worker' }, // Simplification for MVP
    update: { lastHeartbeat: new Date(), status: 'ONLINE' },
    create: { name, status: 'ONLINE' },
  });
  return worker;
});

// 4. Worker: Get Next Job (Polling)
fastify.get('/v1/worker/next-job', async (request, reply) => {
  const job = await prisma.job.findFirst({
    where: { status: 'PENDING' },
    orderBy: { createdAt: 'asc' },
  });
  if (!job) return reply.status(204).send();
  return job;
});

// 5. Worker: Update Job Progress/Result
fastify.patch('/v1/jobs/:id', async (request, reply) => {
  const { id } = request.params as { id: string };
  try {
    const data = UpdateJobSchema.parse(request.body);
    
    const updateData: any = {};
    if (data.status) updateData.status = data.status;
    if (data.resultData) updateData.resultData = data.resultData;

    const job = await prisma.job.update({
      where: { id },
      data: updateData,
    });

    if (data.event) {
      await prisma.jobEvent.create({
        data: {
          jobId: id,
          eventType: data.event.type,
          message: data.event.message,
          payload: data.event.payload,
        },
      });
    }

    // Trigger Telegram if completed
    if (data.status === 'COMPLETED') {
      const tgToken = process.env.TELEGRAM_BOT_TOKEN;
      const chatId = process.env.TELEGRAM_CHAT_ID;

      if (tgToken && chatId) {
        logger.info(`Job ${id} completed. Triggering Telegram...`);
        const summary = data.resultData?.summary || 'Tugas selesai tanpa ringkasan.';
        const message = `✅ *Tugas Selesai*\n\n*ID:* \`${id}\`\n*Workflow:* ${job.workflowName}\n*Hasil:* ${summary}`;
        
        axios.post(`https://api.telegram.org/bot${tgToken}/sendMessage`, {
          chat_id: chatId,
          text: message,
          parse_mode: 'Markdown'
        }).catch(err => logger.error(`Telegram error: ${err.message}`));
      }
    }

    return job;
  } catch (err) {
    reply.status(400).send({ error: (err as Error).message });
  }
});

const start = async () => {
  try {
    await fastify.listen({ port: 3000, host: '0.0.0.0' });
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

start();
