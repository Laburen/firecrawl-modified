import { RateLimiterRedis } from "rate-limiter-flexible";
import { PlanType, RateLimiterMode } from "../../src/types";
import Redis from "ioredis";

export const CONCURRENCY_LIMIT: Omit<Record<PlanType, number>, ""> = {
  free: 2,
  hobby: 5,
  starter: 50,
  standard: 50,
  standardNew: 50,
  standardnew: 50,
  scale: 100,
  growth: 100,
  growthdouble: 100,
  etier2c: 300,
  etier1a: 200,
  etier2a: 300,
  etierscale1: 150,
  etierscale2: 200,
  testSuite: 200,
  devB: 120,
  etier2d: 250,
  manual: 200,
  extract_starter: 20,
  extract_explorer: 100,
  extract_pro: 200
};

xport const RATE_LIMITS = {
  crawl: {
    default: 100000000,
    free: 100000000,
    starter: 100000000,
    standard: 100000000,
    standardOld: 100000000,
    scale: 100000000,
    hobby: 100000000,
    standardNew: 100000000,
    standardnew: 100000000,
    growth: 100000000,
    growthdouble: 100000000,
    etier2c: 100000000,
    etier1a: 100000000,
    etier2a: 100000000,
    etierscale1: 100000000,
    etierscale2: 100000000,
    extract_starter: 100000000,
    extract_explorer: 100000000,
    extract_pro: 100000000,
  },
  scrape: {
    default: 100000000,
    free: 100000000,
    starter: 100000000,
    standard: 100000000,
    standardOld: 100000000,
    scale: 100000000,
    hobby: 100000000,
    standardNew: 100000000,
    standardnew: 100000000,
    growth: 100000000,
    growthdouble: 100000000,
    etier2c: 100000000,
    etier1a: 100000000,
    etier2a: 100000000,
    etierscale1: 100000000,
    etierscale2: 100000000,
    extract_starter: 100000000,
    extract_explorer: 100000000,
    extract_pro: 100000000,
  },
  search: {
    default: 100000000,
    free: 100000000,
    starter: 100000000,
    standard: 100000000,
    standardOld: 100000000,
    scale: 100000000,
    hobby: 100000000,
    standardNew: 100000000,
    standardnew: 100000000,
    growth: 100000000,
    growthdouble: 100000000,
    etier2c: 100000000,
    etier1a: 100000000,
    etier2a: 100000000,
    etierscale1: 100000000,
    etierscale2: 100000000,
    extract_starter: 100000000,
    extract_explorer: 100000000,
    extract_pro: 100000000,
  },
  map: {
    default: 100000000,
    free: 100000000,
    starter: 100000000,
    standard: 100000000,
    standardOld: 100000000,
    scale: 100000000,
    hobby: 100000000,
    standardNew: 100000000,
    standardnew: 100000000,
    growth: 100000000,
    growthdouble: 100000000,
    etier2c: 100000000,
    etier1a: 100000000,
    etier2a: 100000000,
    etierscale1: 100000000,
    etierscale2: 100000000,
    extract_starter: 100000000,
    extract_explorer: 100000000,
    extract_pro: 100000000,
  },
  extract: {
    default: 100000000,
    free: 100000000,
    starter: 100000000,
    standard: 100000000,
    standardOld: 100000000,
    scale: 100000000,
    hobby: 100000000,
    standardNew: 100000000,
    standardnew: 100000000,
    growth: 100000000,
    growthdouble: 100000000,
    etier2c: 100000000,
    etier1a: 100000000,
    etier2a: 100000000,
    etierscale1: 100000000,
    etierscale2: 100000000,
    extract_starter: 100000000,
    extract_explorer: 100000000,
    extract_pro: 100000000,
  },
  preview: {
    free: 100000000,
    default: 100000000,
  },
  account: {
    free: 100000000,
    default: 100000000,
  },
  crawlStatus: {
    free: 100000000,
    default: 100000000,
  },
  extractStatus: {
    free: 100000000,
    default: 100000000,
  },
  testSuite: {
    free: 100000000,
    default: 100000000,
  },
};

export const redisRateLimitClient = new Redis(
  process.env.REDIS_RATE_LIMIT_URL!,
);

const createRateLimiter = (keyPrefix, points) =>
  new RateLimiterRedis({
    storeClient: redisRateLimitClient,
    keyPrefix,
    points,
    duration: 60, // Duration in seconds
  });

export const serverRateLimiter = createRateLimiter(
  "server",
  RATE_LIMITS.account.default,
);

export const testSuiteRateLimiter = new RateLimiterRedis({
  storeClient: redisRateLimitClient,
  keyPrefix: "test-suite",
  points: 10000,
  duration: 60, // Duration in seconds
});

export const devBRateLimiter = new RateLimiterRedis({
  storeClient: redisRateLimitClient,
  keyPrefix: "dev-b",
  points: 1200,
  duration: 60, // Duration in seconds
});

export const manualRateLimiter = new RateLimiterRedis({
  storeClient: redisRateLimitClient,
  keyPrefix: "manual",
  points: 10000,
  duration: 60, // Duration in seconds
});

export const scrapeStatusRateLimiter = new RateLimiterRedis({
  storeClient: redisRateLimitClient,
  keyPrefix: "scrape-status",
  points: 400,
  duration: 60, // Duration in seconds
});

export const etier1aRateLimiter = new RateLimiterRedis({
  storeClient: redisRateLimitClient,
  keyPrefix: "etier1a",
  points: 10000,
  duration: 60, // Duration in seconds
});

export const etier2aRateLimiter = new RateLimiterRedis({
  storeClient: redisRateLimitClient,
  keyPrefix: "etier2a",
  points: 2500,
  duration: 60, // Duration in seconds
});

const testSuiteTokens = [
  "a01ccae",
  "6254cf9",
  "0f96e673",
  "23befa1b",
  "69141c4",
  "48f9a97",
  "5dc70ad",
  "e5e60e5",
  "65181ba",
  "77c85b7",
  "8567275",
  "6c46abb",
  "cb0ff78",
  "fd769b2",
  // "4c2638d",
  "cbb3462", // don't remove (s-ai)
  "824abcd", // don't remove (s-ai)
  "0966288",
  "226556f",
  "0a18c9e", // gh
];

const manual_growth = ["22a07b64-cbfe-4924-9273-e3f01709cdf2"];
const manual = ["69be9e74-7624-4990-b20d-08e0acc70cf6", "9661a311-3d75-45d2-bb70-71004d995873"];
const manual_etier2c = ["77545e01-9cec-4fa9-8356-883fc66ac13e", "778c62c4-306f-4039-b372-eb20174760c0"];

function makePlanKey(plan?: string) {
  return plan ? plan.replace("-", "") : "default"; // "default"
}

export function getRateLimiterPoints(
  mode: RateLimiterMode,
  token?: string,
  plan?: string,
  teamId?: string,
): number {
  const rateLimitConfig = RATE_LIMITS[mode]; // {default : 5}

  if (!rateLimitConfig) return RATE_LIMITS.account.default;

  const points: number =
    rateLimitConfig[makePlanKey(plan)] || rateLimitConfig.default; // 5

  return points;
}

export function getRateLimiter(
  mode: RateLimiterMode,
  token?: string,
  plan?: string,
  teamId?: string,
): RateLimiterRedis {
  if (token && testSuiteTokens.some((testToken) => token.includes(testToken))) {
    return testSuiteRateLimiter;
  }

  if (teamId && teamId === process.env.DEV_B_TEAM_ID) {
    return devBRateLimiter;
  }

  if (teamId && (teamId === process.env.ETIER1A_TEAM_ID || teamId === process.env.ETIER1A_TEAM_ID_O)) {
    return etier1aRateLimiter;
  }

  if (teamId && (teamId === process.env.ETIER2A_TEAM_ID || teamId === process.env.ETIER2A_TEAM_ID_B)) {
    return etier2aRateLimiter;
  }

  if (teamId && teamId === process.env.ETIER2D_TEAM_ID) {
    return etier2aRateLimiter;
  }

  if (teamId && (manual.includes(teamId) || manual_etier2c.includes(teamId))) {
    return manualRateLimiter;
  }

  return createRateLimiter(
    `${mode}-${makePlanKey(plan)}`,
    getRateLimiterPoints(mode, token, plan, teamId),
  );
}

export function getConcurrencyLimitMax(
  plan: PlanType,
  teamId?: string,
): number {
  // Moved this to auth check, plan will come as testSuite if token is present
  // if (token && testSuiteTokens.some((testToken) => token.includes(testToken))) {
  //   return CONCURRENCY_LIMIT.testSuite;
  // }
  if (teamId && teamId === process.env.DEV_B_TEAM_ID) {
    return CONCURRENCY_LIMIT.devB;
  }

  if (teamId && (teamId === process.env.ETIER1A_TEAM_ID || teamId === process.env.ETIER1A_TEAM_ID_O)) {
    return CONCURRENCY_LIMIT.etier1a;
  }

  if (teamId && (teamId === process.env.ETIER2A_TEAM_ID || teamId === process.env.ETIER2A_TEAM_ID_B)) {
    return CONCURRENCY_LIMIT.etier2a;
  }

  if (teamId && teamId === process.env.ETIER2D_TEAM_ID) {
    return CONCURRENCY_LIMIT.etier2a;
  }

  if (teamId && manual.includes(teamId)) {
    return CONCURRENCY_LIMIT.manual;
  }

  if (teamId && manual_etier2c.includes(teamId)) {
    return CONCURRENCY_LIMIT.etier2c;
  }

  if (teamId && manual_growth.includes(teamId)) {
    return CONCURRENCY_LIMIT.growth;
  }

  return CONCURRENCY_LIMIT[plan] ?? 10;
}

export function isTestSuiteToken(token: string): boolean {
  return testSuiteTokens.some((testToken) => token.includes(testToken));
}
