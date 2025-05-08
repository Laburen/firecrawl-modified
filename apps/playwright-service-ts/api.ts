import express, { Request, Response } from 'express';
import bodyParser from 'body-parser';
import { chromium, Browser, BrowserContext, Route, Request as PlaywrightRequest, Page } from 'playwright';
import dotenv from 'dotenv';
import UserAgent from 'user-agents';
import { getError } from './helpers/get_error';

dotenv.config();

const app = express();
const port = process.env.PORT || 3003;

app.use(bodyParser.json());

const BLOCK_MEDIA = (process.env.BLOCK_MEDIA || 'False').toUpperCase() === 'TRUE';
const PROXY_SERVER = process.env.PROXY_SERVER || null;
const PROXY_USERNAME = process.env.PROXY_USERNAME || null;
const PROXY_PASSWORD = process.env.PROXY_PASSWORD || null;

const AD_SERVING_DOMAINS = [
  'doubleclick.net',
  'adservice.google.com',
  'googlesyndication.com',
  'googletagservices.com',
  'googletagmanager.com',
  'google-analytics.com',
  'adsystem.com',
  'adservice.com',
  'adnxs.com',
  'ads-twitter.com',
  'facebook.net',
  'fbcdn.net',
  'amazon-adsystem.com'
];

interface UrlModel {
  url: string;
  wait_after_load?: number;
  timeout?: number;
  headers?: { [key: string]: string };
  check_selector?: string;
}

let browser: Browser;
let context: BrowserContext;

const createContextOptions = () => {
  const userAgent = new UserAgent().toString();
  const viewport = { width: 1280, height: 800 };

  const contextOptions: any = { userAgent, viewport };

  if (PROXY_SERVER && PROXY_USERNAME && PROXY_PASSWORD) {
    contextOptions.proxy = {
      server: PROXY_SERVER,
      username: PROXY_USERNAME,
      password: PROXY_PASSWORD,
    };
  } else if (PROXY_SERVER) {
    contextOptions.proxy = { server: PROXY_SERVER };
  }

  return contextOptions;
};

const initializeBrowser = async () => {
  browser = await chromium.launch({
    headless: true,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-accelerated-2d-canvas',
      '--no-first-run',
      '--no-zygote',
      '--single-process',
      '--disable-gpu'
    ]
  });

  context = await browser.newContext(createContextOptions());

  if (BLOCK_MEDIA) {
    await context.route('**/*.{png,jpg,jpeg,gif,svg,mp3,mp4,avi,flac,ogg,wav,webm}', async (route: Route) => {
      await route.abort();
    });
  }

  await context.route('**/*', (route: Route, request: PlaywrightRequest) => {
    const requestUrl = new URL(request.url());
    const hostname = requestUrl.hostname;
    if (AD_SERVING_DOMAINS.some(domain => hostname.includes(domain))) {
      console.log(`❌ Blocked: ${hostname}`);
      return route.abort();
    }
    return route.continue();
  });
};

const shutdownBrowser = async () => {
  if (context) await context.close();
  if (browser) await browser.close();
};

const isValidUrl = (urlString: string): boolean => {
  try {
    new URL(urlString);
    return true;
  } catch (_) {
    return false;
  }
};

const scrapePage = async (
  page: Page,
  url: string,
  waitUntil: 'load' | 'networkidle',
  waitAfterLoad: number,
  timeout: number,
  checkSelector?: string
) => {
  console.log(`Navigating to ${url} with waitUntil: ${waitUntil} and timeout: ${timeout}ms`);
  const response = await page.goto(url, { waitUntil, timeout });

  if (waitAfterLoad > 0) {
    await page.waitForTimeout(waitAfterLoad);
  }

  if (checkSelector) {
    try {
      await page.waitForSelector(checkSelector, { timeout });
    } catch {
      throw new Error('Required selector not found');
    }
  }

  let content = await page.content();
  let headers = null;

  if (response) {
    headers = await response.allHeaders();
    const ct = Object.entries(headers).find(([key]) => key.toLowerCase() === 'content-type');
    if (ct && (ct[1].includes('application/json') || ct[1].includes('text/plain'))) {
      content = (await response.body()).toString('utf8');
    }
  }

  return { content, status: response?.status() || null, headers };
};

// ✅ Healthcheck endpoint
app.get('/healthcheck', async (_:Request, res:Response) => {
  try {
    const testPage = await context.newPage();
    await testPage.goto('https://www.firecrawl.dev/', { timeout: 5000 });
    await testPage.close();
    res.sendStatus(200);
  } catch (e) {
    console.error('❌ Healthcheck failed:', e);
    res.sendStatus(500);
  }
});


app.post('/scrape', async (req: Request, res: Response) => {
  const { url, wait_after_load = 0, timeout = 15000, headers, check_selector }: UrlModel = req.body;

  console.log(`================= Scrape Request =================`);
  console.log(`URL: ${url}`);
  console.log(`Wait After Load: ${wait_after_load}`);
  console.log(`Timeout: ${timeout}`);
  console.log(`Headers: ${headers ? JSON.stringify(headers) : 'None'}`);
  console.log(`Check Selector: ${check_selector || 'None'}`);
  console.log(`==================================================`);

  if (!url) return res.status(400).json({ error: 'URL is required' });
  if (!isValidUrl(url)) return res.status(400).json({ error: 'Invalid URL' });

  if (!PROXY_SERVER) {
    console.warn('⚠️ WARNING: No proxy server provided. Your IP address may be blocked.');
  }

  if (!browser || !context) {
    await initializeBrowser();
  }

  // ✅ Verificamos la cantidad de páginas abiertas para evitar saturar
  const MAX_PAGES = 30;
  if (context.pages().length >= MAX_PAGES) {
    console.warn(`♻️ Reiniciando contexto porque hay ${context.pages().length} pestañas abiertas`);
    await context.close();
    context = await browser.newContext(createContextOptions());
  }

  const page = await context.newPage();

  try {
    if (headers) {
      await page.setExtraHTTPHeaders(headers);
    }

    let result;
    try {
      console.log('🔍 Attempting strategy 1: Normal load');
      result = await scrapePage(page, url, 'load', wait_after_load, timeout, check_selector);
    } catch {
      console.log('🔁 Strategy 1 failed, trying strategy 2: networkidle');
      result = await scrapePage(page, url, 'networkidle', wait_after_load, timeout, check_selector);
    }

    const pageError = result.status !== 200 ? getError(result.status) : undefined;

    if (!pageError) console.log('✅ Scrape successful!');
    else console.log(`🚨 Scrape failed: ${result.status} ${pageError}`);

    return res.json({
      content: result.content,
      pageStatusCode: result.status,
      ...(pageError && { pageError })
    });
  } catch (err) {
    console.error('❌ Unexpected error in scrape:', err);
    return res.status(500).json({ error: 'An unexpected error occurred' });
  } finally {
    await page.close();
  }
});

app.listen(port, () => {
  initializeBrowser().then(() => {
    console.log(`🚀 Server running on port ${port}`);
  });
});

process.on('SIGINT', () => {
  shutdownBrowser().then(() => {
    console.log('🧹 Browser closed on SIGINT');
    process.exit(0);
  });
});
