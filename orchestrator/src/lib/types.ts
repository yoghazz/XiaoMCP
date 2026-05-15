export type JobStatus = 'PENDING' | 'RUNNING' | 'COMPLETED' | 'FAILED' | 'TIMEOUT';
export type EventType = 'PROGRESS' | 'LOG' | 'ERROR';

export interface IJobCreate {
  workflowName: string;
  inputParams: any;
  correlationId?: string;
}

export interface IJobUpdate {
  status?: JobStatus;
  resultData?: any;
  event?: {
    type: EventType;
    message: string;
    payload?: any;
  };
}

export interface IJob {
  id: string;
  correlationId?: string | null;
  workflowName: string;
  inputParams: string;
  status: string;
  resultData?: string | null;
  createdAt: Date;
  updatedAt: Date;
}
