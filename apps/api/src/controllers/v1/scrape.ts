import { Response } from "express";
import { logger } from "../../lib/logger";
import {
  Document,
  RequestWithAuth,
  ScrapeRequest,
  scrapeRequestSchema,
  ScrapeResponse,
} from "./types";
import { billTeam } from "../../services/billing/credit_billing";
import { v4 as uuidv4 } from "uuid";
import { addScrapeJob, waitForJob } from "../../services/queue-jobs";
import { logJob } from "../../services/logging/log_job";
import { getJobPriority } from "../../lib/job-priority";
import { PlanType } from "../../types";
import { getScrapeQueue } from "../../services/queue-service";

export async function scrapeController(
  req: RequestWithAuth<{}, ScrapeResponse, ScrapeRequest>,
  res: Response<ScrapeResponse>,
) {
  const jobId = uuidv4();
  const preNormalizedBody = { ...req.body };

  console.log("🔵 [SCRAPE INIT]", JSON.stringify({
    scrapeId: jobId,
    request: req.body,
    originalRequest: preNormalizedBody,
    teamId: req.auth.team_id,
    account: req.account,
  }));

  logger.debug("Scrape " + jobId + " starting", {
    scrapeId: jobId,
    request: req.body,
    originalRequest: preNormalizedBody,
    teamId: req.auth.team_id,
    account: req.account,
  });

  try {
    req.body = scrapeRequestSchema.parse(req.body);
  } catch (error) {
    console.error("❌ [SCHEMA ERROR]", error);
    return res.status(400).json({
      success: false,
      error: "Invalid request format",
    });
  }

  let earlyReturn = false;

  const origin = req.body.origin;
  const timeout = req.body.timeout;
  const startTime = new Date().getTime();

  console.log("🕒 [TIMEOUT]", timeout);
  console.log("🟢 [AUTH INFO]", JSON.stringify(req.auth));

  const jobPriority = await getJobPriority({
    plan: req.auth.plan as PlanType,
    team_id: req.auth.team_id,
    basePriority: 10,
  });

  console.log("⚙️ [JOB PRIORITY]", jobPriority);

  await addScrapeJob(
    {
      url: req.body.url,
      mode: "single_urls",
      team_id: req.auth.team_id,
      scrapeOptions: req.body,
      internalOptions: { teamId: req.auth.team_id },
      plan: req.auth.plan!,
      origin: req.body.origin,
      is_scrape: true,
    },
    {},
    jobId,
    jobPriority,
  );

  console.log("📤 [JOB ADDED TO QUEUE]", jobId);

  const totalWait =
    (req.body.waitFor ?? 0) +
    (req.body.actions ?? []).reduce(
      (a, x) => (x.type === "wait" ? (x.milliseconds ?? 0) : 0) + a,
      0,
    );

  console.log("⏱️ [TOTAL WAIT TIME]", totalWait);

  let doc: Document;

  try {
    console.log("⏳ [WAITING FOR JOB]", jobId, `Timeout: ${timeout + totalWait}ms`);
    doc = await waitForJob<Document>(jobId, timeout + totalWait);
    console.log("✅ [JOB COMPLETED]", jobId);
  } catch (e) {
    logger.error(`Error in scrapeController: ${e}`, {
      jobId,
      scrapeId: jobId,
      startTime,
    });
    console.error("❌ [WAIT ERROR]", e);

    if (
      e instanceof Error &&
      (e.message.startsWith("Job wait") || e.message === "timeout")
    ) {
      return res.status(408).json({
        success: false,
        error: "Request timed out",
      });
    } else {
      return res.status(500).json({
        success: false,
        error: `(Internal server error) - ${e && e.message ? e.message : e}`,
      });
    }
  }

  await getScrapeQueue().remove(jobId);
  console.log("🗑️ [JOB REMOVED FROM QUEUE]", jobId);

  const endTime = new Date().getTime();
  const timeTakenInSeconds = (endTime - startTime) / 1000;

  console.log("⏱️ [TIME TAKEN]", timeTakenInSeconds, "seconds");

  const numTokens =
    doc && doc.extract
      ? 0 // TODO: calculate actual tokens
      : 0;

  let creditsToBeBilled = 1;
  if (earlyReturn) {
    console.log("↩️ [EARLY RETURN] Skipping billing and response");
    return;
  }
  if (req.body.extract && req.body.formats.includes("extract")) {
    creditsToBeBilled = 5;
  }

  console.log("💳 [BILLING CREDITS]", creditsToBeBilled);

  billTeam(req.auth.team_id, req.acuc?.sub_id, creditsToBeBilled).catch(
    (error) => {
      console.error("❌ [BILLING ERROR]", error);
      logger.error(
        `Failed to bill team ${req.auth.team_id} for ${creditsToBeBilled} credits: ${error}`,
      );
    },
  );

  if (!req.body.formats.includes("rawHtml")) {
    if (doc && doc.rawHtml) {
      console.log("🚫 [REMOVING RAW HTML]");
      delete doc.rawHtml;
    }
  }

  logJob({
    job_id: jobId,
    success: true,
    message: "Scrape completed",
    num_docs: 1,
    docs: [doc],
    time_taken: timeTakenInSeconds,
    team_id: req.auth.team_id,
    mode: "scrape",
    url: req.body.url,
    scrapeOptions: req.body,
    origin: origin,
    num_tokens: numTokens,
  });

  console.log("📦 [RETURNING RESPONSE]", {
    success: true,
    scrape_id: origin?.includes("website") ? jobId : undefined,
  });

  return res.status(200).json({
    success: true,
    data: doc,
    scrape_id: origin?.includes("website") ? jobId : undefined,
  });
}
