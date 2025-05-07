// services/concurrency-processor.ts
import {
    cleanOldConcurrencyLimitEntries,
    getConcurrencyLimitActiveJobs,
    takeConcurrencyLimitedJob
  } from "../lib/concurrency-limit";
  import { _addScrapeJobToBullMQ } from "./queue-jobs"
  import { logger } from "../lib/logger";
  
  export async function processConcurrencyQueue(teamId: string, maxConcurrency: number) {
    const now = Date.now();
    cleanOldConcurrencyLimitEntries(teamId, now);
    const activeCount = (await getConcurrencyLimitActiveJobs(teamId, now)).length;
    const slots = Math.max(maxConcurrency - activeCount, 0);
  
    for (let i = 0; i < slots; i++) {
      const job = await takeConcurrencyLimitedJob(teamId);
      if (!job) break;
      logger.info(`🔄 Rescatando job ${job.id} de la concurrency-queue para ${teamId}`);
      await _addScrapeJobToBullMQ(job.data, job.opts, job.id, job.priority = 10);
    }
  }
  