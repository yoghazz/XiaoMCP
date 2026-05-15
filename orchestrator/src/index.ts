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

const API_KEY = process.env.API_KEY || 'change-me';
const HOST = process.env.HOST || '0.0.0.0';
const PORT = Number(process.env.PORT || 3000);

fastify.addHook('preHandler', async (request, reply) => {
  if (request.url === '/healthz') return;
  const apiKey = request.headers['x-api-key'];
  if (!apiKey || apiKey !== API_KEY) {
    return reply.status(401).send({ error: 'Unauthorized' });
  }
});

const CreateJobSchema = z.object({
  workflowName: z.string().min(1),
  inputParams: z.record(z.any()).default({}),
  correlationId: z.string().min(1).optional(),
  source: z.string().min(1).optional(),
  userId: z.string().min(1).optional(),
});

const HeartbeatSchema = z.object({
  workerId: z.string().optional(),
  name: z.string().min(1),
});

const ClaimJobSchema = z.object({
  workerId: z.string().optional(),
  workerName: z.string().min(1),
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

async function sendTelegramCompletion(jobId: string, workflowName: string, resultData: any) {
  const tgToken = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;
  if (!tgToken || !chatId) return;

  const summary = resultData?.summary || 'Tugas selesai tanpa ringkasan.';
  const message = `✅ *Tugas Selesai*\n\n*ID:* \`${jobId}\`\n*Workflow:* ${workflowName}\n*Hasil:* ${summary}`;

  try {
    await axios.post(`https://api.telegram.org/bot${tgToken}/sendMessage`, {
      chat_id: chatId,
      text: message,
      parse_mode: 'Markdown'
    }, { timeout: 10000 });
  } catch (err) {
    logger.error({ err }, 'Telegram notification failed');
  }
}

fastify.get('/healthz', async () => ({ ok: true }));

fastify.post('/v1/jobs', async (request, reply) => {
  try {
    const data = CreateJobSchema.parse(request.body);

    if (data.correlationId) {
      const existing = await prisma.job.findUnique({ where: { correlationId: data.correlationId } });
      if (existing) {
        return { job_id: existing.id, status: existing.status, deduplicated: true };
      }
    }

    const job = await prisma.job.create({
      data: {
        workflowName: data.workflowName,
        inputParams: data.inputParams,
        correlationId: data.correlationId,
        source: data.source,
        userId: data.userId,
        status: 'PENDING',
      },
    });

    return { job_id: job.id, status: job.status, deduplicated: false };
  } catch (err) {
    return reply.status(400).send({ error: (err as Error).message });
  }
});

fastify.get('/v1/jobs/:id', async (request, reply) => {
  const { id } = request.params as { id: string };
  const job = await prisma.job.findUnique({
    where: { id },
    include: { events: { orderBy: { createdAt: 'desc' }, take: 20 }, worker: true }
  });
  if (!job) return reply.status(404).send({ error: 'Job not found' });
  return job;
});

fastify.get('/v1/jobs/latest', async (request, reply) => {
  const workflowName = typeof (request.query as any)?.workflowName === 'string'
    ? String((request.query as any).workflowName)
    : undefined;
  const job = await prisma.job.findFirst({
    where: { status: 'COMPLETED', ...(workflowName ? { workflowName } : {}) },
    orderBy: { updatedAt: 'desc' },
  });
  if (!job) return reply.status(404).send({ error: 'Belum ada tugas yang selesai.' });
  return {
    id: job.id,
    workflowName: job.workflowName,
    summary: (job.resultData as any)?.summary || 'Tugas selesai.'
  };
});

fastify.post('/v1/worker/heartbeat', async (request, reply) => {
  try {
    const data = HeartbeatSchema.parse(request.body);
    const worker = await prisma.worker.upsert({
      where: data.workerId ? { id: data.workerId } : { name: data.name },
      update: { name: data.name, lastHeartbeat: new Date(), status: 'ONLINE' },
      create: { name: data.name, status: 'ONLINE' },
    });
    return worker;
  } catch (err) {
    return reply.status(400).send({ error: (err as Error).message });
  }
});

fastify.post('/v1/worker/claim-next', async (request, reply) => {
  try {
    const data = ClaimJobSchema.parse(request.body);
    const worker = await prisma.worker.upsert({
      where: data.workerId ? { id: data.workerId } : { name: data.workerName },
      update: { name: data.workerName, lastHeartbeat: new Date(), status: 'ONLINE' },
      create: { name: data.workerName, status: 'ONLINE' },
    });

    for (let i = 0; i < 5; i++) {
      const candidate = await prisma.job.findFirst({
        where: { status: 'PENDING' },
        orderBy: { createdAt: 'asc' },
      });

      if (!candidate) return reply.status(204).send();

      const claimed = await prisma.job.updateMany({
        where: { id: candidate.id, status: 'PENDING' },
        data: { status: 'RUNNING', workerId: worker.id },
      });

      if (claimed.count === 1) {
        await prisma.jobEvent.create({
          data: {
            jobId: candidate.id,
            eventType: 'PROGRESS',
            message: `Job claimed by worker ${worker.name}`,
          }
        });

        const fullJob = await prisma.job.findUnique({ where: { id: candidate.id } });
        return fullJob;
      }
    }

    return reply.status(409).send({ error: 'Failed to claim job, retry.' });
  } catch (err) {
    return reply.status(400).send({ error: (err as Error).message });
  }
});

fastify.patch('/v1/jobs/:id', async (request, reply) => {
  const { id } = request.params as { id: string };
  try {
    const data = UpdateJobSchema.parse(request.body);

    const updateData: Record<string, any> = {};
    if (data.status) updateData.status = data.status;
    if (typeof data.resultData !== 'undefined') updateData.resultData = data.resultData;

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

    if (data.status === 'COMPLETED') {
      await sendTelegramCompletion(id, job.workflowName, data.resultData);
    }

    return job;
  } catch (err) {
    return reply.status(400).send({ error: (err as Error).message });
  }
});

const start = async () => {
  try {
    await fastify.listen({ port: PORT, host: HOST });
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

start();
