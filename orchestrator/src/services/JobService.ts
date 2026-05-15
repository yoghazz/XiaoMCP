import { PrismaClient } from '@prisma/client';
import { IJobCreate, IJobUpdate } from '../lib/types';
import axios from 'axios';
import pino from 'pino';

const logger = pino();
const prisma = new PrismaClient();

export class JobService {
  async createJob(data: IJobCreate) {
    return await prisma.job.create({
      data: {
        workflowName: data.workflowName,
        inputParams: JSON.stringify(data.inputParams),
        correlationId: data.correlationId,
        status: 'PENDING',
      },
    });
  }

  async getJob(id: string) {
    return await prisma.job.findUnique({
      where: { id },
      include: { events: { orderBy: { createdAt: 'desc' }, take: 10 } }
    });
  }

  async getLatestJob() {
    return await prisma.job.findFirst({
      where: { status: 'COMPLETED' },
      orderBy: { updatedAt: 'desc' },
    });
  }

  async updateJob(id: string, data: IJobUpdate) {
    const updateData: any = {};
    if (data.status) updateData.status = data.status;
    if (data.resultData) updateData.resultData = JSON.stringify(data.resultData);

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
          payload: data.event.payload ? JSON.stringify(data.event.payload) : null,
        },
      });
    }

    if (data.status === 'COMPLETED') {
      await this.notifyCompletion(job);
    }

    return job;
  }

  async claimNextJob() {
    return await prisma.job.findFirst({
      where: { status: 'PENDING' },
      orderBy: { createdAt: 'asc' },
    });
  }

  private async notifyCompletion(job: any) {
    const tgToken = process.env.TELEGRAM_BOT_TOKEN;
    const chatId = process.env.TELEGRAM_CHAT_ID;

    if (tgToken && chatId) {
      const result = job.resultData ? JSON.parse(job.resultData) : {};
      const summary = result.summary || 'Tugas selesai.';
      const message = `✅ *Tugas Selesai*\n\n*Workflow:* ${job.workflowName}\n*Hasil:* ${summary}`;
      
      await axios.post(`https://api.telegram.org/bot${tgToken}/sendMessage`, {
        chat_id: chatId,
        text: message,
        parse_mode: 'Markdown'
      }).catch(err => logger.error(`Telegram error: ${err.message}`));
    }
  }

  async heartbeat(name: string, workerId?: string) {
    return await prisma.worker.upsert({
      where: { id: workerId || 'local-default' },
      update: { lastHeartbeat: new Date(), status: 'ONLINE' },
      create: { name, status: 'ONLINE' },
    });
  }
}

export const jobService = new JobService();
