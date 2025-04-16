import "dotenv/config";
import "./sentry";
import * as Sentry from "@sentry/node";
import { CustomError } from "../lib/custom-error";
import {
  getScrapeQueue,
  getExtractQueue,
  getDeepResearchQueue,
  redisConnection,
  scrapeQueueName,
  extractQueueName,
  deepResearchQueueName,
  getIndexQueue,
  getGenerateLlmsTxtQueue,
  getBillingQueue,
} from "./queue-service";
import { startWebScraperPipeline } from "../main/runWebScraper";
import { callWebhook } from "./webhook";
import { logJob } from "./logging/log_job";
import { Job, Queue } from "bullmq";
import { logger as _logger } from "../lib/logger";
import { Worker } from "bullmq";
import systemMonitor from "./system-monitor";
import { v4 as uuidv4 } from "uuid";
import {
  addCrawlJob,
  addCrawlJobDone,
  addCrawlJobs,
  crawlToCrawler,
  finishCrawl,
  finishCrawlPre,
  finishCrawlKickoff,
  generateURLPermutations,
  getCrawl,
  getCrawlJobCount,
  getCrawlJobs,
  getDoneJobsOrderedLength,
  lockURL,
  lockURLs,
  lockURLsIndividually,
  normalizeURL,
  saveCrawl,
} from "../lib/crawl-redis";
import { StoredCrawl } from "../lib/crawl-redis";
import { addScrapeJob, addScrapeJobs } from "./queue-jobs";
import {
  addJobPriority,
  deleteJobPriority,
  getJobPriority,
} from "../../src/lib/job-priority";
import { PlanType, RateLimiterMode } from "../types";
import { getJobs } from "..//controllers/v1/crawl-status";
import { configDotenv } from "dotenv";
import { scrapeOptions } from "../controllers/v1/types";
import { getRateLimiterPoints } from "./rate-limiter";
import {
  cleanOldConcurrencyLimitEntries,
  pushConcurrencyLimitActiveJob,
  removeConcurrencyLimitActiveJob,
  takeConcurrencyLimitedJob,
} from "../lib/concurrency-limit";
import { isUrlBlocked } from "../scraper/WebScraper/utils/blocklist";
import { BLOCKLISTED_URL_MESSAGE } from "../lib/strings";
import { indexPage } from "../lib/extract/index/pinecone";
import { Document } from "../controllers/v1/types";
import { performExtraction } from "../lib/extract/extraction-service";
import { supabase_service } from "../services/supabase";
import { normalizeUrl, normalizeUrlOnlyHostname } from "../lib/canonical-url";
import { saveExtract, updateExtract } from "../lib/extract/extract-redis";
import { billTeam } from "./billing/credit_billing";
import { saveCrawlMap } from "./indexing/crawl-maps-index";
import { updateDeepResearch } from "../lib/deep-research/deep-research-redis";
import { performDeepResearch } from "../lib/deep-research/deep-research-service";
import { performGenerateLlmsTxt } from "../lib/generate-llmstxt/generate-llmstxt-service";
import { updateGeneratedLlmsTxt } from "../lib/generate-llmstxt/generate-llmstxt-redis";

configDotenv();

class RacedRedirectError extends Error {
  constructor() {
    super("Raced redirect error");
  }
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const workerLockDuration = Number(process.env.WORKER_LOCK_DURATION) || 60000;
const workerStalledCheckInterval =
  Number(process.env.WORKER_STALLED_CHECK_INTERVAL) || 30000;
const jobLockExtendInterval =
  Number(process.env.JOB_LOCK_EXTEND_INTERVAL) || 15000;
const jobLockExtensionTime =
  Number(process.env.JOB_LOCK_EXTENSION_TIME) || 60000;

const cantAcceptConnectionInterval =
  Number(process.env.CANT_ACCEPT_CONNECTION_INTERVAL) || 7000;
const connectionMonitorInterval =
  Number(process.env.CONNECTION_MONITOR_INTERVAL) || 1000;

const gotJobInterval = Number(process.env.CONNECTION_MONITOR_INTERVAL) || 1000;
console.log("[CONNECTION_MONITOR_INTERVAL]", gotJobInterval);
console.log("[CANT_ACCEPT_CONNECTION_INTERVAL]", cantAcceptConnectionInterval);

const runningJobs: Set<string> = new Set();

// Función finishCrawlIfNeeded: Se ejecuta para determinar si el crawl (rastreo) ha finalizado
async function finishCrawlIfNeeded(job: Job & { id: string }, sc: StoredCrawl) {
  console.log(`>> Entering finishCrawlIfNeeded for job: ${job.id}`);
  if (await finishCrawlPre(job.data.crawl_id)) {
    if (job.data.crawlerOptions && !await redisConnection.exists("crawl:" + job.data.crawl_id + ":invisible_urls")) {
      await redisConnection.set("crawl:" + job.data.crawl_id + ":invisible_urls", "done", "EX", 60 * 60 * 24);
      const scLocal = (await getCrawl(job.data.crawl_id))!;
      const visitedUrls = new Set(await redisConnection.smembers("crawl:" + job.data.crawl_id + ":visited_unique"));
      const lastUrls: string[] = ((await supabase_service.rpc("diff_get_last_crawl_urls", {
        i_team_id: job.data.team_id,
        i_url: scLocal.originUrl!,
      })).data ?? []).map(x => x.url);
      const lastUrlsSet = new Set(lastUrls);
      const crawler = crawlToCrawler(
        job.data.crawl_id,
        scLocal,
        scLocal.originUrl!,
        job.data.crawlerOptions,
      );
      const univistedUrls = crawler.filterLinks(
        Array.from(lastUrlsSet).filter(x => !visitedUrls.has(x)),
        Infinity,
        scLocal.crawlerOptions.maxDepth ?? 10,
      );
      
      const addableJobCount = scLocal.crawlerOptions.limit === undefined ? Infinity : (scLocal.crawlerOptions.limit - await getDoneJobsOrderedLength(job.data.crawl_id));
      console.log("finishCrawlIfNeeded - Origin URL:", scLocal.originUrl!, " | Unvisited URLs:", univistedUrls, " | Visited URLs:", visitedUrls, " | Last URLs:", lastUrls, " | Addable Job Count:", addableJobCount);
      
      if (univistedUrls.length !== 0 && addableJobCount > 0) {
        const jobs = univistedUrls.slice(0, addableJobCount).map((url) => {
          const uuid = uuidv4();
          return {
            name: uuid,
            data: {
              url,
              mode: "single_urls" as const,
              team_id: job.data.team_id,
              plan: job.data.plan!,
              crawlerOptions: {
                ...job.data.crawlerOptions,
                urlInvisibleInCurrentCrawl: true,
              },
              scrapeOptions: job.data.scrapeOptions,
              internalOptions: scLocal.internalOptions,
              origin: job.data.origin,
              crawl_id: job.data.crawl_id,
              sitemapped: true,
              webhook: job.data.webhook,
              v1: job.data.v1,
            },
            opts: {
              jobId: uuid,
              priority: 20,
            },
          };
        });
        console.log("finishCrawlIfNeeded - Generated jobs for unvisited URLs:", jobs.map(j => j.opts.jobId));
        const lockedIds = await lockURLsIndividually(
          job.data.crawl_id,
          scLocal,
          jobs.map((x) => ({ id: x.opts.jobId, url: x.data.url })),
        );
        const lockedJobs = jobs.filter((x) =>
          lockedIds.find((y) => y.id === x.opts.jobId),
        );
        console.log("finishCrawlIfNeeded - Locked Job IDs:", lockedIds);
        await addCrawlJobs(
          job.data.crawl_id,
          lockedJobs.map((x) => x.opts.jobId),
        );
        await addScrapeJobs(lockedJobs);
        console.log(`finishCrawlIfNeeded - Queued ${lockedJobs.length} new scrape jobs`);
        return;
      }
    }
    await finishCrawl(job.data.crawl_id);
    console.log(`finishCrawlIfNeeded - Finished crawl ${job.data.crawl_id}`);
    (async () => {
      const originUrl = sc.originUrl ? normalizeUrlOnlyHostname(sc.originUrl) : undefined;
      const visitedUrls = await redisConnection.smembers("crawl:" + job.data.crawl_id + ":visited_unique");
      if (visitedUrls.length > 0 && job.data.crawlerOptions !== null && originUrl) {
        await getIndexQueue().add(
          job.data.crawl_id,
          {
            originUrl,
            visitedUrls,
          },
          {
            priority: 10,
          },
        );
        console.log("finishCrawlIfNeeded - Indexing job added for crawl", job.data.crawl_id);
      }
    })();
    if (!job.data.v1) {
      const jobIDs = await getCrawlJobs(job.data.crawl_id);
      const jobs = (await getJobs(jobIDs)).sort((a, b) => a.timestamp - b.timestamp);
      const jobStatus = sc.cancelled ? "failed" : "completed";
      const fullDocs = jobs
        .map((x) =>
          x.returnvalue
            ? Array.isArray(x.returnvalue)
              ? x.returnvalue[0]
              : x.returnvalue
            : null,
        )
        .filter((x) => x !== null);
      await logJob({
        job_id: job.data.crawl_id,
        success: jobStatus === "completed",
        message: sc.cancelled ? "Cancelled" : undefined,
        num_docs: fullDocs.length,
        docs: [],
        time_taken: (Date.now() - sc.createdAt) / 1000,
        team_id: job.data.team_id,
        mode: job.data.crawlerOptions !== null ? "crawl" : "batch_scrape",
        url: sc.originUrl!,
        scrapeOptions: sc.scrapeOptions,
        crawlerOptions: sc.crawlerOptions,
        origin: job.data.origin,
      });
      console.log("finishCrawlIfNeeded - Logged job with status:", jobStatus);
      const data = {
        success: jobStatus !== "failed",
        result: {
          links: fullDocs.map((doc) => {
            return {
              content: doc,
              source: doc?.metadata?.sourceURL ?? doc?.url ?? "",
            };
          }),
        },
        project_id: job.data.project_id,
        docs: fullDocs,
      };
      if (!job.data.v1) {
        callWebhook(
          job.data.team_id,
          job.data.crawl_id,
          data,
          job.data.webhook,
          job.data.v1,
          job.data.crawlerOptions !== null ? "crawl.completed" : "batch_scrape.completed",
        );
        console.log("finishCrawlIfNeeded - Called webhook (v0) for crawl.completed");
      }
    } else {
      const num_docs = await getDoneJobsOrderedLength(job.data.crawl_id);
      const jobStatus = sc.cancelled ? "failed" : "completed";
      await logJob(
        {
          job_id: job.data.crawl_id,
          success: jobStatus === "completed",
          message: sc.cancelled ? "Cancelled" : undefined,
          num_docs,
          docs: [],
          time_taken: (Date.now() - sc.createdAt) / 1000,
          team_id: job.data.team_id,
          scrapeOptions: sc.scrapeOptions,
          mode: job.data.crawlerOptions !== null ? "crawl" : "batch_scrape",
          url: sc?.originUrl ?? (job.data.crawlerOptions === null ? "Batch Scrape" : "Unknown"),
          crawlerOptions: sc.crawlerOptions,
          origin: job.data.origin,
        },
        true,
      );
      if (job.data.v1 && job.data.webhook) {
        callWebhook(
          job.data.team_id,
          job.data.crawl_id,
          [],
          job.data.webhook,
          job.data.v1,
          job.data.crawlerOptions !== null ? "crawl.completed" : "batch_scrape.completed",
        );
        console.log("finishCrawlIfNeeded - Called webhook (v1) for crawl.completed");
      }
    }
  }
  console.log(`<< Exiting finishCrawlIfNeeded for job: ${job.id}`);
}

const processJobInternal = async (token: string, job: Job & { id: string }) => {
  console.log(`>> Entering processJobInternal for job: ${job.id}`);
  const logger = _logger.child({
    module: "queue-worker",
    method: "processJobInternal",
    jobId: job.id,
    scrapeId: job.id,
    crawlId: job.data?.crawl_id ?? undefined,
  });
  const extendLockInterval = setInterval(async () => {
    logger.info(`🐂 Worker extending lock on job ${job.id}`, {
      extendInterval: jobLockExtendInterval,
      extensionTime: jobLockExtensionTime,
    });
    console.log(`Extending lock on job ${job.id}`);
    if (job.data?.mode !== "kickoff" && job.data?.team_id) {
      await pushConcurrencyLimitActiveJob(job.data.team_id, job.id, 60 * 1000);
    }
    await job.extendLock(token, jobLockExtensionTime);
  }, jobLockExtendInterval);
  await addJobPriority(job.data.team_id, job.id);
  let err = null;
  try {
    if (job.data?.mode === "kickoff") {
      console.log(`Job ${job.id} is a kickoff job`);
      const result = await processKickoffJob(job, token);
      if (result.success) {
        console.log(`Job ${job.id} kickoff processed successfully`);
        try {
          await job.moveToCompleted(null, token, false);
        } catch (e) {
          console.error(`Error al mover job ${job.id} a completed: ${e}`);
        }
      } else {
        logger.debug("Job failed", { result, mode: job.data.mode });
        await job.moveToFailed((result as any).error, token, false);
      }
    } else {
      console.log(`Processing regular job ${job.id}`);
      const result = await processJob(job, token);
      if (result.success) {
        console.log(`Job ${job.id} processed successfully`);
        try {
          if (job.data.crawl_id && process.env.USE_DB_AUTHENTICATION === "true") {
            logger.debug("Job succeeded -- has crawl associated, putting null in Redis");
            await job.moveToCompleted(null, token, false);
          } else {
            logger.debug("Job succeeded -- putting result in Redis");
            await job.moveToCompleted(result.document, token, false);
          }
        } catch (e) {
          console.error(`Error al mover job ${job.id} a completed: ${e}`);
        }
      } else {
        logger.debug("Job failed", { result });
        await job.moveToFailed((result as any).error, token, false);
      }
    }
  } catch (error) {
    logger.debug("Job failed", { error });
    console.error(`Error en processJobInternal for job ${job.id}:`, error);
    Sentry.captureException(error);
    err = error;
    await job.moveToFailed(error, token, false);
  } finally {
    await deleteJobPriority(job.data.team_id, job.id);
    clearInterval(extendLockInterval);
    console.log(`<< Exiting processJobInternal for job: ${job.id}`);
  }
  return err;
};

const processExtractJobInternal = async (token: string, job: Job & { id: string }) => {
  console.log(`>> Entering processExtractJobInternal for job: ${job.id}`);
  const logger = _logger.child({
    module: "extract-worker",
    method: "processJobInternal",
    jobId: job.id,
    extractId: job.data.extractId,
    teamId: job.data?.teamId ?? undefined,
  });
  const extendLockInterval = setInterval(async () => {
    logger.info(`🔄 Worker extending lock on job ${job.id}`);
    console.log(`Extending lock on extract job ${job.id}`);
    await job.extendLock(token, jobLockExtensionTime);
  }, jobLockExtendInterval);
  try {
    const result = await performExtraction(job.data.extractId, {
      request: job.data.request,
      teamId: job.data.teamId,
      plan: job.data.plan,
      subId: job.data.subId,
    });
    console.log(`Extraction result for job ${job.id}:`, result);
    if (result.success) {
      await job.moveToCompleted(result, token, false);
      console.log(`Extract job ${job.id} completed successfully`);
      return result;
    } else {
      await job.moveToCompleted(result, token, false);
      await updateExtract(job.data.extractId, {
        status: "failed",
        error: result.error ?? "Unknown error, please contact help@firecrawl.com. Extract id: " + job.data.extractId,
      });
      console.error(`Extraction for job ${job.id} failed:`, result.error);
      return result;
    }
  } catch (error) {
    logger.error(`🚫 Job errored ${job.id} - ${error}`, { error });
    console.error(`Error in processExtractJobInternal for job ${job.id}:`, error);
    Sentry.captureException(error, { data: { job: job.id } });
    try {
      await job.moveToFailed(error, token, false);
    } catch (e) {
      logger.log("Failed to move job to failed state in Redis", { error: e });
    }
    await updateExtract(job.data.extractId, {
      status: "failed",
      error: error.error ?? error ?? "Unknown error, please contact help@firecrawl.com. Extract id: " + job.data.extractId,
    });
    return {
      success: false,
      error: error.error ?? error ?? "Unknown error, please contact help@firecrawl.com. Extract id: " + job.data.extractId,
    };
  } finally {
    clearInterval(extendLockInterval);
    console.log(`<< Exiting processExtractJobInternal for job: ${job.id}`);
  }
};

const processDeepResearchJobInternal = async (token: string, job: Job & { id: string }) => {
  console.log(`>> Entering processDeepResearchJobInternal for job: ${job.id}`);
  const logger = _logger.child({
    module: "deep-research-worker",
    method: "processJobInternal",
    jobId: job.id,
    researchId: job.data.researchId,
    teamId: job.data?.teamId ?? undefined,
  });
  const extendLockInterval = setInterval(async () => {
    logger.info(`🔄 Worker extending lock on job ${job.id}`);
    console.log(`Extending lock on deep research job ${job.id}`);
    await job.extendLock(token, jobLockExtensionTime);
  }, jobLockExtendInterval);
  try {
    console.log(`[Deep Research] Starting deep research for researchId: `, job.data.researchId);
    const result = await performDeepResearch({
      researchId: job.data.researchId,
      teamId: job.data.teamId,
      plan: job.data.plan,
      query: job.data.request.query,
      maxDepth: job.data.request.maxDepth,
      timeLimit: job.data.request.timeLimit,
      subId: job.data.subId,
      maxUrls: job.data.request.maxUrls,
      analysisPrompt: job.data.request.analysisPrompt,
      systemPrompt: job.data.request.systemPrompt,
      formats: job.data.request.formats,
      jsonOptions: job.data.request.jsonOptions,
    });
    console.log(`Deep research result for job ${job.id}:`, result);
    if(result.success) {
      await job.moveToCompleted(result, token, false);
      console.log(`Deep research job ${job.id} completed successfully`);
      return result;
    } else {
      const error = new Error("Deep research failed without specific error");
      await updateDeepResearch(job.data.researchId, {
        status: "failed",
        error: error.message,
      });
      await job.moveToFailed(error, token, false);
      console.error(`Deep research job ${job.id} failed:`, error.message);
      return { success: false, error: error.message };
    }
  } catch (error) {
    logger.error(`🚫 Job errored ${job.id} - ${error}`, { error });
    console.error(`Error in processDeepResearchJobInternal for job ${job.id}:`, error);
    Sentry.captureException(error, { data: { job: job.id } });
    try {
      await job.moveToFailed(error, token, false);
    } catch (e) {
      logger.error("Failed to move job to failed state in Redis", { error: e });
    }
    await updateDeepResearch(job.data.researchId, {
      status: "failed",
      error: error.message || "Unknown error occurred",
    });
    return { success: false, error: error.message || "Unknown error occurred" };
  } finally {
    clearInterval(extendLockInterval);
    console.log(`<< Exiting processDeepResearchJobInternal for job: ${job.id}`);
  }
};

const processGenerateLlmsTxtJobInternal = async (token: string, job: Job & { id: string }) => {
  console.log(`>> Entering processGenerateLlmsTxtJobInternal for job: ${job.id}`);
  const logger = _logger.child({
    module: "generate-llmstxt-worker",
    method: "processJobInternal",
    jobId: job.id,
    generateId: job.data.generateId,
    teamId: job.data?.teamId ?? undefined,
  });
  const extendLockInterval = setInterval(async () => {
    logger.info(`🔄 Worker extending lock on job ${job.id}`);
    console.log(`Extending lock on generateLlmsTxt job ${job.id}`);
    await job.extendLock(token, jobLockExtensionTime);
  }, jobLockExtendInterval);
  try {
    const result = await performGenerateLlmsTxt({
      generationId: job.data.generationId,
      teamId: job.data.teamId,
      plan: job.data.plan,
      url: job.data.request.url,
      maxUrls: job.data.request.maxUrls,
      showFullText: job.data.request.showFullText,
      subId: job.data.subId,
    });
    console.log(`GenerateLlmsTxt result for job ${job.id}:`, result);
    if (result.success) {
      await job.moveToCompleted(result, token, false);
      await updateGeneratedLlmsTxt(job.data.generateId, {
        status: "completed",
        generatedText: result.data.generatedText,
        fullText: result.data.fullText,
      });
      console.log(`GenerateLlmsTxt job ${job.id} completed successfully`);
      return result;
    } else {
      const error = new Error("LLMs text generation failed without specific error");
      await job.moveToFailed(error, token, false);
      await updateGeneratedLlmsTxt(job.data.generateId, {
        status: "failed",
        error: error.message,
      });
      console.error(`GenerateLlmsTxt job ${job.id} failed:`, error.message);
      return { success: false, error: error.message };
    }
  } catch (error) {
    logger.error(`🚫 Job errored ${job.id} - ${error}`, { error });
    console.error(`Error in processGenerateLlmsTxtJobInternal for job ${job.id}:`, error);
    Sentry.captureException(error, { data: { job: job.id } });
    try {
      await job.moveToFailed(error, token, false);
    } catch (e) {
      logger.error("Failed to move job to failed state in Redis", { error: e });
    }
    await updateGeneratedLlmsTxt(job.data.generateId, {
      status: "failed", 
      error: error.message || "Unknown error occurred",
    });
    return { success: false, error: error.message || "Unknown error occurred" };
  } finally {
    clearInterval(extendLockInterval);
    console.log(`<< Exiting processGenerateLlmsTxtJobInternal for job: ${job.id}`);
  }
};

let isShuttingDown = false;

process.on("SIGINT", () => {
  console.log("Received SIGINT. Shutting down gracefully...");
  isShuttingDown = true;
});

process.on("SIGTERM", () => {
  console.log("Received SIGTERM. Shutting down gracefully...");
  isShuttingDown = true;
});

let cantAcceptConnectionCount = 0;

const workerFun = async (
  queue: Queue,
  processJobInternal: (token: string, job: Job) => Promise<any>,
) => {
  console.log(`>> Starting workerFun for queue: ${queue.name}`);
  const logger = _logger.child({ module: "queue-worker", method: "workerFun" });
  const worker = new Worker(queue.name, null, {
    connection: redisConnection,
    lockDuration: 1 * 60 * 1000, // 1 minute
    stalledInterval: 30 * 1000, // 30 seconds
    maxStalledCount: 10, // 10 times
  });
  worker.startStalledCheckTimer();
  console.log(`Worker for queue ${queue.name} started with lock duration ${workerLockDuration}`);

  const monitor = await systemMonitor;
  while (true) {
    if (isShuttingDown) {
      console.log("No longer accepting new jobs. SIGINT received. Exiting worker loop.");
      break;
    }
    const token = uuidv4();
    const canAcceptConnection = await monitor.acceptConnection();
    if (!canAcceptConnection) {
      console.log(`Can't accept connection due to RAM/CPU load. Waiting for ${cantAcceptConnectionInterval}ms`);
      logger.info(`Can't accept connection due to RAM/CPU load (${cantAcceptConnectionInterval})`);
      cantAcceptConnectionCount++;
      if (cantAcceptConnectionCount >= 25) {
        logger.error("WORKER STALLED", {
          cpuUsage: await monitor.checkCpuUsage(),
          memoryUsage: await monitor.checkMemoryUsage(),
        });
      }
      await sleep(cantAcceptConnectionInterval);
      continue;
    } else {
      cantAcceptConnectionCount = 0;
    }
    const job = await worker.getNextJob(token);
    if (job) {
      console.log(`Job ${job.id} obtained from queue ${queue.name}`);
      if (job.id) {
        runningJobs.add(job.id);
      }
      async function afterJobDone(job: Job<any, any, string>) {
        if (job.id) {
          runningJobs.delete(job.id);
          console.log(`Job ${job.id} removed from runningJobs set`);
        }
        if (job.id && job.data && job.data.team_id && job.data.plan) {
          await removeConcurrencyLimitActiveJob(job.data.team_id, job.id);
          cleanOldConcurrencyLimitEntries(job.data.team_id);
          const nextJob = await takeConcurrencyLimitedJob(job.data.team_id);
          if (nextJob !== null) {
            await pushConcurrencyLimitActiveJob(job.data.team_id, nextJob.id, 60 * 1000);
            await queue.add(
              nextJob.id,
              {
                ...nextJob.data,
                concurrencyLimitHit: true,
              },
              {
                ...nextJob.opts,
                jobId: nextJob.id,
                priority: nextJob.priority,
              },
            );
            console.log(`Queued next job ${nextJob.id} after finishing job ${job.id}`);
          }
        }
      }
      if (job.data && job.data.sentry && Sentry.isInitialized()) {
        Sentry.continueTrace(
          {
            sentryTrace: job.data.sentry.trace,
            baggage: job.data.sentry.baggage,
          },
          () => {
            Sentry.startSpan(
              {
                name: "Scrape job",
                attributes: {
                  job: job.id,
                  worker: process.env.FLY_MACHINE_ID ?? worker.id,
                },
              },
              async (span) => {
                await Sentry.startSpan(
                  {
                    name: "Process scrape job",
                    op: "queue.process",
                    attributes: {
                      "messaging.message.id": job.id,
                      "messaging.destination.name": getScrapeQueue().name,
                      "messaging.message.body.size": job.data.sentry.size,
                      "messaging.message.receive.latency": Date.now() - (job.processedOn ?? job.timestamp),
                      "messaging.message.retry.count": job.attemptsMade,
                    },
                  },
                  async () => {
                    let res;
                    try {
                      res = await processJobInternal(token, job);
                    } finally {
                      await afterJobDone(job);
                    }
                    if (res !== null) {
                      span.setStatus({ code: 2 }); // ERROR
                    } else {
                      span.setStatus({ code: 1 }); // OK
                    }
                  },
                );
              },
            );
          },
        );
      } else {
        Sentry.startSpan(
          {
            name: "Scrape job",
            attributes: {
              job: job.id,
              worker: process.env.FLY_MACHINE_ID ?? worker.id,
            },
          },
          () => {
            processJobInternal(token, job).finally(() => afterJobDone(job));
          },
        );
      }
      await sleep(gotJobInterval);
    } else {
      await sleep(connectionMonitorInterval);
    }
  }
  console.log(`<< Exiting workerFun for queue: ${queue.name}`);
};

async function processKickoffJob(job: Job & { id: string }, token: string) {
  console.log(`>> Entering processKickoffJob for job: ${job.id}`);
  const logger = _logger.child({
    module: "queue-worker",
    method: "processKickoffJob",
    jobId: job.id,
    scrapeId: job.id,
    crawlId: job.data?.crawl_id ?? undefined,
    teamId: job.data?.team_id ?? undefined,
  });
  try {
    const sc = (await getCrawl(job.data.crawl_id)) as StoredCrawl;
    const crawler = crawlToCrawler(job.data.crawl_id, sc);
    logger.debug("Locking URL...");
    console.log("processKickoffJob - Locking URL:", job.data.url);
    await lockURL(job.data.crawl_id, sc, job.data.url);
    const jobId = uuidv4();
    logger.debug("Adding scrape job to Redis...", { jobId });
    console.log("processKickoffJob - Adding scrape job with jobId:", jobId);
    await addScrapeJob(
      {
        url: job.data.url,
        mode: "single_urls",
        team_id: job.data.team_id,
        crawlerOptions: job.data.crawlerOptions,
        scrapeOptions: scrapeOptions.parse(job.data.scrapeOptions),
        internalOptions: sc.internalOptions,
        plan: job.data.plan!,
        origin: job.data.origin,
        crawl_id: job.data.crawl_id,
        webhook: job.data.webhook,
        v1: job.data.v1,
        isCrawlSourceScrape: true,
      },
      {
        priority: 15,
      },
      jobId,
    );
    logger.debug("Adding scrape job to BullMQ...", { jobId });
    console.log("processKickoffJob - Queuing job in BullMQ with jobId:", jobId);
    await addCrawlJob(job.data.crawl_id, jobId);
    if (job.data.webhook) {
      logger.debug("Calling webhook with crawl.started...", { webhook: job.data.webhook });
      console.log("processKickoffJob - Calling webhook crawl.started");
      await callWebhook(
        job.data.team_id,
        job.data.crawl_id,
        null,
        job.data.webhook,
        true,
        "crawl.started",
      );
    }
    const sitemap = sc.crawlerOptions.ignoreSitemap
      ? 0
      : await crawler.tryGetSitemap(async (urls) => {
          if (urls.length === 0) return;
          logger.debug("Using sitemap chunk of length " + urls.length, { sitemapLength: urls.length });
          console.log("processKickoffJob - Sitemap chunk length:", urls.length);
          let jobPriority = await getJobPriority({
            plan: job.data.plan,
            team_id: job.data.team_id,
            basePriority: 21,
          });
          logger.debug("Using job priority " + jobPriority, { jobPriority });
          const jobs = urls.map((url) => {
            const uuid = uuidv4();
            return {
              name: uuid,
              data: {
                url,
                mode: "single_urls" as const,
                team_id: job.data.team_id,
                plan: job.data.plan!,
                crawlerOptions: job.data.crawlerOptions,
                scrapeOptions: job.data.scrapeOptions,
                internalOptions: sc.internalOptions,
                origin: job.data.origin,
                crawl_id: job.data.crawl_id,
                sitemapped: true,
                webhook: job.data.webhook,
                v1: job.data.v1,
              },
              opts: {
                jobId: uuid,
                priority: 20,
              },
            };
          });
          logger.debug("Locking URLs...");
          console.log("processKickoffJob - Locking URLs for sitemap");
          const lockedIds = await lockURLsIndividually(
            job.data.crawl_id,
            sc,
            jobs.map((x) => ({ id: x.opts.jobId, url: x.data.url })),
          );
          const lockedJobs = jobs.filter((x) =>
            lockedIds.find((y) => y.id === x.opts.jobId),
          );
          logger.debug("Adding scrape jobs to Redis...");
          await addCrawlJobs(job.data.crawl_id, lockedJobs.map((x) => x.opts.jobId));
          logger.debug("Adding scrape jobs to BullMQ...");
          await addScrapeJobs(lockedJobs);
          console.log("processKickoffJob - Queued scrape jobs for sitemap");
        });
    if (sitemap === 0) {
      logger.debug("Sitemap not found or ignored.", { ignoreSitemap: sc.crawlerOptions.ignoreSitemap });
      console.log("processKickoffJob - No sitemap used (ignored or not found)");
    }
    logger.debug("Done queueing jobs!");
    await finishCrawlKickoff(job.data.crawl_id);
    await finishCrawlIfNeeded(job, sc);
    console.log(`<< Exiting processKickoffJob for job: ${job.id} with success`);
    return { success: true };
  } catch (error) {
    logger.error("An error occurred!", { error });
    console.error(`Error in processKickoffJob for job ${job.id}:`, error);
    await finishCrawlKickoff(job.data.crawl_id);
    const sc = (await getCrawl(job.data.crawl_id)) as StoredCrawl;
    if (sc) {
      await finishCrawlIfNeeded(job, sc);
    }
    console.log(`<< Exiting processKickoffJob for job: ${job.id} with error`);
    return { success: false, error };
  }
}

async function indexJob(job: Job & { id: string }, document: Document) {
  console.log(`>> Entering indexJob for job: ${job.id}`);
  if (document && document.markdown && job.data.team_id === process.env.BACKGROUND_INDEX_TEAM_ID!) {
    // indexPage({ ... });
    console.log(`indexJob - Indexing document for job: ${job.id}`);
  }
  console.log(`<< Exiting indexJob for job: ${job.id}`);
}

async function processJob(job: Job & { id: string }, token: string) {
  console.log(`>> Entering processJob for job: ${job.id}`);
  const logger = _logger.child({
    module: "queue-worker",
    method: "processJob",
    jobId: job.id,
    scrapeId: job.id,
    crawlId: job.data?.crawl_id ?? undefined,
    teamId: job.data?.team_id ?? undefined,
  });
  logger.info(`🐂 Worker taking job ${job.id}`, { url: job.data.url });
  const start = Date.now();
  try {
    job.updateProgress({
      current: 1,
      total: 100,
      current_step: "SCRAPING",
      current_url: "",
    });
    if (job.data.crawl_id) {
      const sc = (await getCrawl(job.data.crawl_id)) as StoredCrawl;
      if (sc && sc.cancelled) {
        throw new Error("Parent crawl/batch scrape was cancelled");
      }
    }
    console.log(`processJob - Starting pipeline for job ${job.id}`);
    const pipeline = await Promise.race([
      startWebScraperPipeline({ job, token }),
      ...(job.data.scrapeOptions.timeout !== undefined
        ? [
            (async () => {
              await sleep(job.data.scrapeOptions.timeout);
              throw new Error("timeout");
            })(),
          ]
        : []),
    ]);
    if (!pipeline.success) {
      throw pipeline.error;
    }
    const end = Date.now();
    const timeTakenInSeconds = (end - start) / 1000;
    const doc = pipeline.document;
    const rawHtml = doc.rawHtml ?? "";
    if (!job.data.scrapeOptions.formats.includes("rawHtml")) {
      delete doc.rawHtml;
      console.log(`processJob - rawHtml removed for job ${job.id}`);
    }
    if (job.data.concurrencyLimited) {
      doc.warning = "This scrape job was throttled at your current concurrency limit. If you'd like to scrape faster, you can upgrade your plan." + (doc.warning ? " " + doc.warning : "");
    }
    const data = {
      success: true,
      result: {
        links: [
          {
            content: doc,
            source: doc?.metadata?.sourceURL ?? doc?.metadata?.url ?? "",
            id: job.id,
          },
        ],
      },
      project_id: job.data.project_id,
      document: doc,
    };
    if (job.data.webhook && job.data.mode !== "crawl" && job.data.v1) {
      logger.debug("Calling webhook with success...", { webhook: job.data.webhook });
      console.log(`processJob - Calling webhook for job ${job.id}`);
      await callWebhook(
        job.data.team_id,
        job.data.crawl_id,
        data,
        job.data.webhook,
        job.data.v1,
        job.data.crawlerOptions !== null ? "crawl.page" : "batch_scrape.page",
        true,
      );
    }
    if (job.data.crawl_id) {
      const sc = (await getCrawl(job.data.crawl_id)) as StoredCrawl;
      if (
        doc.metadata.url !== undefined &&
        doc.metadata.sourceURL !== undefined &&
        normalizeURL(doc.metadata.url, sc) !== normalizeURL(doc.metadata.sourceURL, sc) &&
        job.data.crawlerOptions !== null
      ) {
        const crawler = crawlToCrawler(job.data.crawl_id, sc);
        if (crawler.filterURL(doc.metadata.url, doc.metadata.sourceURL) === null && !job.data.isCrawlSourceScrape) {
          throw new Error("Redirected target URL is not allowed by crawlOptions");
        }
        const isHostnameDifferent = normalizeUrlOnlyHostname(doc.metadata.url) !== normalizeUrlOnlyHostname(doc.metadata.sourceURL);
        if (job.data.isCrawlSourceScrape && isHostnameDifferent) {
          sc.originUrl = doc.metadata.url;
          await saveCrawl(job.data.crawl_id, sc);
        }
        if (isUrlBlocked(doc.metadata.url)) {
          throw new Error(BLOCKLISTED_URL_MESSAGE);
        }
        const p1 = generateURLPermutations(normalizeURL(doc.metadata.url, sc));
        const p2 = generateURLPermutations(normalizeURL(doc.metadata.sourceURL, sc));
        if (JSON.stringify(p1) !== JSON.stringify(p2)) {
          logger.debug("Was redirected, removing old URL and locking new URL...", { oldUrl: doc.metadata.sourceURL, newUrl: doc.metadata.url });
          const x = await redisConnection.sadd("crawl:" + job.data.crawl_id + ":visited", ...p1.map((x) => x.href));
          const lockRes = x === p1.length;
          if (job.data.crawlerOptions !== null && !lockRes) {
            throw new RacedRedirectError();
          }
        }
      }
      logger.debug("Logging job to DB...");
      await logJob(
        {
          job_id: job.id as string,
          success: true,
          num_docs: 1,
          docs: [doc],
          time_taken: timeTakenInSeconds,
          team_id: job.data.team_id,
          mode: job.data.mode,
          url: job.data.url,
          crawlerOptions: sc.crawlerOptions,
          scrapeOptions: job.data.scrapeOptions,
          origin: job.data.origin,
          crawl_id: job.data.crawl_id,
        },
        true,
      );
      console.log(`processJob - Logged job ${job.id}`);
      indexJob(job, doc);
      logger.debug("Declaring job as done...");
      await addCrawlJobDone(job.data.crawl_id, job.id, true);
      if (job.data.crawlerOptions !== null) {
        if (!sc.cancelled) {
          const crawler = crawlToCrawler(
            job.data.crawl_id,
            sc,
            doc.metadata.url ?? doc.metadata.sourceURL ?? sc.originUrl!,
            job.data.crawlerOptions,
          );
          const links = crawler.filterLinks(
            await crawler.extractLinksFromHTML(rawHtml ?? "", doc.metadata?.url ?? doc.metadata?.sourceURL ?? sc.originUrl!),
            Infinity,
            sc.crawlerOptions?.maxDepth ?? 10,
          );
          logger.debug("Discovered " + links.length + " links...", { linksLength: links.length });
          console.log(`processJob - ${links.length} links discovered for job ${job.id}`);
          for (const link of links) {
            if (await lockURL(job.data.crawl_id, sc, link)) {
              const jobPriority = await getJobPriority({
                plan: sc.plan as PlanType,
                team_id: sc.team_id,
                basePriority: job.data.crawl_id ? 20 : 10,
              });
              const jobId = uuidv4();
              logger.debug("Determined job priority " + jobPriority + " for URL " + JSON.stringify(link), { jobPriority, url: link });
              await addScrapeJob(
                {
                  url: link,
                  mode: "single_urls",
                  team_id: sc.team_id,
                  scrapeOptions: scrapeOptions.parse(sc.scrapeOptions),
                  internalOptions: sc.internalOptions,
                  crawlerOptions: {
                    ...sc.crawlerOptions,
                    currentDiscoveryDepth: (job.data.crawlerOptions?.currentDiscoveryDepth ?? 0) + 1,
                  },
                  plan: job.data.plan,
                  origin: job.data.origin,
                  crawl_id: job.data.crawl_id,
                  webhook: job.data.webhook,
                  v1: job.data.v1,
                },
                {},
                jobId,
                jobPriority,
              );
              await addCrawlJob(job.data.crawl_id, jobId);
              logger.debug("Added job for URL " + JSON.stringify(link), { jobPriority, url: link, newJobId: jobId });
            }
          }
          if (job.data.isCrawlSourceScrape && crawler.filterLinks([doc.metadata.url ?? doc.metadata.sourceURL!], 1, sc.crawlerOptions?.maxDepth ?? 10).length === 0) {
            throw new Error("Source URL is not allowed by includePaths/excludePaths rules");
          }
        }
      }
      await finishCrawlIfNeeded(job, sc);
    } else {
      indexJob(job, doc);
    }
    if (job.data.is_scrape !== true) {
      let creditsToBeBilled = 1; // Se asume 1 crédito por documento
      if (job.data.scrapeOptions.extract) {
        creditsToBeBilled = 5;
      }
      if (job.data.team_id !== process.env.BACKGROUND_INDEX_TEAM_ID! && process.env.USE_DB_AUTHENTICATION === "true") {
        try {
          const billingJobId = uuidv4();
          logger.debug(`Adding billing job to queue for team ${job.data.team_id}`, { billingJobId, credits: creditsToBeBilled, is_extract: false });
          console.log(`processJob - Adding billing job ${billingJobId} for team ${job.data.team_id} with ${creditsToBeBilled} credit(s)`);
          await getBillingQueue().add(
            "bill_team",
            {
              team_id: job.data.team_id,
              subscription_id: undefined,
              credits: creditsToBeBilled,
              is_extract: false,
              timestamp: new Date().toISOString(),
              originating_job_id: job.id
            },
            {
              jobId: billingJobId,
              priority: 10,
            }
          );
        } catch (error) {
          logger.error(`Failed to add billing job to queue for team ${job.data.team_id} for ${creditsToBeBilled} credits`, { error });
          console.error(`processJob - Error adding billing job for team ${job.data.team_id}:`, error);
          Sentry.captureException(error);
        }
      }
    }
    logger.info(`🐂 Job done ${job.id}`);
    console.log(`processJob - Completed job ${job.id}`);
    return data;
  } catch (error) {
    if (job.data.crawl_id) {
      const sc = (await getCrawl(job.data.crawl_id)) as StoredCrawl;
      logger.debug("Declaring job as done (failure case)...");
      await addCrawlJobDone(job.data.crawl_id, job.id, false);
      await redisConnection.srem("crawl:" + job.data.crawl_id + ":visited_unique", normalizeURL(job.data.url, sc));
      await finishCrawlIfNeeded(job, sc);
    }
    const isEarlyTimeout = error instanceof Error && error.message === "timeout";
    const isCancelled = error instanceof Error && error.message === "Parent crawl/batch scrape was cancelled";
    if (isEarlyTimeout) {
      logger.error(`🐂 Job timed out ${job.id}`);
      console.error(`processJob - Job ${job.id} timed out`);
    } else if (error instanceof RacedRedirectError) {
      logger.warn(`🐂 Job got redirect raced ${job.id}, silently failing`);
      console.warn(`processJob - Redirect race occurred for job ${job.id}`);
    } else if (isCancelled) {
      logger.warn(`🐂 Job got cancelled, silently failing`);
      console.warn(`processJob - Job ${job.id} was cancelled`);
    } else {
      logger.error(`🐂 Job errored ${job.id} - ${error}`, { error });
      console.error(`processJob - Error processing job ${job.id}:`, error);
      Sentry.captureException(error, { data: { job: job.id } });
      if (error instanceof CustomError) {
        logger.error(error.message);
      }
      logger.error(error);
      if (error.stack) {
        logger.error(error.stack);
      }
    }
    const data = {
      success: false,
      document: null,
      project_id: job.data.project_id,
      error: error instanceof Error
        ? error
        : typeof error === "string"
          ? new Error(error)
          : new Error(JSON.stringify(error)),
    };
    if (!job.data.v1 && (job.data.mode === "crawl" || job.data.crawl_id)) {
      callWebhook(
        job.data.team_id,
        job.data.crawl_id ?? (job.id as string),
        data,
        job.data.webhook,
        job.data.v1,
        job.data.crawlerOptions !== null ? "crawl.page" : "batch_scrape.page",
      );
      console.log(`processJob - Called webhook for failure on job ${job.id}`);
    }
    const end = Date.now();
    const timeTakenInSeconds = (end - start) / 1000;
    logger.debug("Logging failed job to DB...");
    await logJob(
      {
        job_id: job.id as string,
        success: false,
        message: typeof error === "string" ? error : (error.message ?? "Something went wrong... Contact help@mendable.ai"),
        num_docs: 0,
        docs: [],
        time_taken: timeTakenInSeconds,
        team_id: job.data.team_id,
        mode: job.data.mode,
        url: job.data.url,
        crawlerOptions: job.data.crawlerOptions,
        scrapeOptions: job.data.scrapeOptions,
        origin: job.data.origin,
        crawl_id: job.data.crawl_id,
      },
      true,
    );
    console.log(`processJob - Exiting with error for job ${job.id}`);
    return data;
  }
}

// Start all workers
(async () => {
  console.log(">> Starting all workers...");
  await Promise.all([
    workerFun(getScrapeQueue(), processJobInternal),
    workerFun(getExtractQueue(), processExtractJobInternal),
    workerFun(getDeepResearchQueue(), processDeepResearchJobInternal),
    workerFun(getGenerateLlmsTxtQueue(), processGenerateLlmsTxtJobInternal),
  ]);
  console.log("All workers exited. Waiting for all jobs to finish...");
  while (runningJobs.size > 0) {
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  console.log("All jobs finished. Worker out!");
  process.exit(0);
})();
