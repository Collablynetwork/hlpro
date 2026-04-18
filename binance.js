const axios = require("axios");
const config = require("./config");

const rest = axios.create({
  baseURL: config.hyperliquidApiUrl,
  timeout: 25_000,
});

const intervalMsMap = {
  "1m": 60_000,
  "5m": 5 * 60_000,
  "15m": 15 * 60_000,
  "30m": 30 * 60_000,
  "1h": 60 * 60_000,
  "2h": 2 * 60 * 60_000,
  "4h": 4 * 60 * 60_000,
};

const supportedIntervals = new Set(Object.keys(intervalMsMap));

function normalizeInterval(interval) {
  const raw = String(interval || "").trim();
  if (supportedIntervals.has(raw)) return raw;
  if (raw === "6h") return "8h";
  return "1h";
}

const cache = {
  meta: { value: null, expiresAt: 0 },
  assetCtxs: { value: null, expiresAt: 0 },
  allMids: { value: null, expiresAt: 0 },
  candles: new Map(),
  funding: new Map(),
  l2: new Map(),
};

let requestGate = Promise.resolve();
let lastRequestAt = 0;
let throttleUntil = 0;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function now() {
  return Date.now();
}

function jitter(ms = 100) {
  return Math.floor(Math.random() * ms);
}

function candleLimitForInterval(interval, requestedLimit) {
  const normalized = normalizeInterval(interval);
  const hardCap = ["1m", "5m", "15m", "30m", "1h", "2h"].includes(normalized) ? 55 : 50;
  return Math.max(50, Math.min(Number(requestedLimit || hardCap), hardCap));
}

function candleCacheTtlMs(interval) {
  const normalized = normalizeInterval(interval);
  const intervalMs = intervalMsMap[normalized] || 60_000;
  return Math.max(15_000, Math.min(intervalMs, 15 * 60_000));
}

async function waitForRequestSlot(minGapMs = 650) {
  const run = async () => {
    const nowMs = Date.now();
    const delay = Math.max(
      0,
      minGapMs - (nowMs - lastRequestAt),
      throttleUntil - nowMs
    );
    if (delay > 0) {
      await sleep(delay);
    }
    lastRequestAt = Date.now();
  };

  const next = requestGate.then(run, run);
  requestGate = next.catch(() => {});
  return next;
}

function toCoin(symbol) {
  return String(symbol || "").trim().toUpperCase().replace(/USDT$/, "");
}

function fromCoin(coin) {
  return `${String(coin || "").trim().toUpperCase()}USDT`;
}

function normalizeNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

async function postInfo(payload, attempt = 1) {
  try {
    await waitForRequestSlot();
    const response = await rest.post("/info", payload);
    return response.data;
  } catch (error) {
    const status = error.response?.status;
    const shouldRetry = attempt < 5 && (!status || status >= 429);
    if (shouldRetry) {
      if (status === 429) {
        throttleUntil = Math.max(throttleUntil, Date.now() + (3_500 * attempt));
      }
      const delay =
        status === 429
          ? (3_000 * attempt) + jitter(750)
          : (500 * attempt) + jitter(250);
      await sleep(delay);
      return postInfo(payload, attempt + 1);
    }
    if (error.response) {
      const details =
        typeof error.response.data === "string"
          ? error.response.data
          : JSON.stringify(error.response.data);
      error.message = `Hyperliquid /info ${payload?.type || "request"} failed with ${status}: ${details}`;
    }
    throw error;
  }
}

async function getMeta() {
  if (cache.meta.value && cache.meta.expiresAt > now()) {
    return cache.meta.value;
  }

  const meta = await postInfo({ type: "meta" });
  cache.meta = {
    value: meta,
    expiresAt: now() + 10 * 60_000,
  };
  return meta;
}

async function getMetaAndAssetCtxs() {
  if (cache.assetCtxs.value && cache.assetCtxs.expiresAt > now()) {
    return cache.assetCtxs.value;
  }

  const payload = await postInfo({ type: "metaAndAssetCtxs" });
  cache.assetCtxs = {
    value: payload,
    expiresAt: now() + 60_000,
  };
  return payload;
}

async function getAllMids() {
  if (cache.allMids.value && cache.allMids.expiresAt > now()) {
    return cache.allMids.value;
  }

  const mids = await postInfo({ type: "allMids" });
  cache.allMids = {
    value: mids,
    expiresAt: now() + 60_000,
  };
  return mids;
}

async function getAssetCtxMap() {
  const [meta, contexts] = await getMetaAndAssetCtxs();
  const universe = meta?.universe || [];
  const map = new Map();

  for (let index = 0; index < universe.length; index += 1) {
    const asset = universe[index];
    const ctx = contexts?.[index] || {};
    map.set(String(asset.name || "").toUpperCase(), {
      ...asset,
      ...ctx,
    });
  }

  return map;
}

function mapKline(row) {
  const close = normalizeNumber(row.c);
  const volume = normalizeNumber(row.v);
  return {
    openTime: Number(row.t),
    open: normalizeNumber(row.o),
    high: normalizeNumber(row.h),
    low: normalizeNumber(row.l),
    close,
    volume,
    closeTime: Number(row.T),
    quoteVolume: roundVolume(volume * close),
    tradeCount: Number(row.n || 0),
    takerBuyBase: 0,
    takerBuyQuote: 0,
  };
}

function roundVolume(value) {
  return Math.round(Number(value || 0) * 1e8) / 1e8;
}

function getCacheEntry(map, key) {
  const item = map.get(key);
  if (item && item.expiresAt > now()) return item.value;
  return null;
}

function setCacheEntry(map, key, value, ttlMs) {
  map.set(key, {
    value,
    expiresAt: now() + ttlMs,
  });
  return value;
}

async function ping() {
  await getMeta();
  return { ok: true };
}

async function getExchangeInfo() {
  const meta = await getMeta();
  return {
    symbols: (meta?.universe || []).map((asset) => ({
      symbol: fromCoin(asset.name),
      contractType: "PERPETUAL",
      status: "TRADING",
      quoteAsset: "USDT",
      marginAsset: "USDT",
      baseAsset: asset.name,
      onlyIsolated: Boolean(asset.onlyIsolated),
      maxLeverage: Number(asset.maxLeverage || 0),
      szDecimals: Number(asset.szDecimals || 0),
    })),
  };
}

async function getValidUsdtPerpSet() {
  const info = await getExchangeInfo();
  return new Set((info.symbols || []).map((item) => item.symbol));
}

async function getKlines(symbol, interval, limit = 300, startTime, endTime) {
  const coin = toCoin(symbol);
  const normalizedInterval = normalizeInterval(interval);
  const effectiveLimit = candleLimitForInterval(normalizedInterval, limit);
  const cacheKey = `${coin}:${normalizedInterval}:${effectiveLimit}`;
  const cached = getCacheEntry(cache.candles, cacheKey);
  if (cached) return cached;

  const intervalMs = intervalMsMap[normalizedInterval] || 60_000;
  const effectiveEnd = Number(endTime || now());
  const effectiveStart = Number(startTime || effectiveEnd - intervalMs * effectiveLimit);

  const rows = await postInfo({
    type: "candleSnapshot",
    req: {
      coin,
      interval: normalizedInterval,
      startTime: effectiveStart,
      endTime: effectiveEnd,
    },
  });

  const mapped = (rows || []).slice(-effectiveLimit).map(mapKline);
  return setCacheEntry(cache.candles, cacheKey, mapped, candleCacheTtlMs(normalizedInterval));
}

async function getMarkPrice(symbol) {
  const coin = toCoin(symbol);
  const [ctxMap, mids] = await Promise.all([getAssetCtxMap(), getAllMids()]);
  const ctx = ctxMap.get(coin) || {};

  return {
    symbol,
    markPrice: String(ctx.markPx || ctx.midPx || mids?.[coin] || 0),
    indexPrice: String(ctx.oraclePx || ctx.markPx || ctx.midPx || mids?.[coin] || 0),
  };
}

async function getTickerPrice(symbol) {
  const mark = await getMarkPrice(symbol);
  return {
    symbol,
    price: mark.markPrice,
  };
}

async function getBookTicker(symbol) {
  const coin = toCoin(symbol);
  const cached = getCacheEntry(cache.l2, coin);
  if (cached) return cached;

  const ctxMap = await getAssetCtxMap();
  const ctx = ctxMap.get(coin) || {};
  const impactBid = Array.isArray(ctx.impactPxs) ? ctx.impactPxs[0] : null;
  const impactAsk = Array.isArray(ctx.impactPxs) ? ctx.impactPxs[1] : null;
  const mid = ctx.midPx || ctx.markPx || ctx.oraclePx || 0;
  const bidPx = impactBid || mid;
  const askPx = impactAsk || mid;
  const result = {
    symbol,
    bidPrice: String(bidPx || 0),
    bidQty: String(0),
    askPrice: String(askPx || 0),
    askQty: String(0),
  };

  return setCacheEntry(cache.l2, coin, result, 60_000);
}

async function getFundingRate(symbol, limit = 20) {
  const coin = toCoin(symbol);
  const cacheKey = `${coin}:${limit}`;
  const cached = getCacheEntry(cache.funding, cacheKey);
  if (cached) return cached;

  const ctxMap = await getAssetCtxMap();
  const ctx = ctxMap.get(coin) || {};
  const result = [
    {
      coin,
      fundingRate: String(ctx.funding || 0),
      premium: String(ctx.premium || 0),
      time: now(),
    },
  ].slice(-limit);
  return setCacheEntry(cache.funding, cacheKey, result, 60_000);
}

async function getOpenInterest(symbol) {
  const ctxMap = await getAssetCtxMap();
  const ctx = ctxMap.get(toCoin(symbol)) || {};
  return {
    symbol,
    openInterest: String(ctx.openInterest || 0),
  };
}

async function getOpenInterestHist() {
  return [];
}

async function getTakerLongShortRatio() {
  return [];
}

async function getTopLongShortAccountRatio() {
  return [];
}

async function getGlobalLongShortRatio() {
  return [];
}

async function getRecentAggTrades() {
  return [];
}

function createCombinedKlineSocket() {
  throw new Error("Hyperliquid websocket scanning is not implemented in this module");
}

module.exports = {
  ping,
  getExchangeInfo,
  getValidUsdtPerpSet,
  getKlines,
  getMarkPrice,
  getTickerPrice,
  getBookTicker,
  getFundingRate,
  getOpenInterest,
  getOpenInterestHist,
  getTakerLongShortRatio,
  getTopLongShortAccountRatio,
  getGlobalLongShortRatio,
  getRecentAggTrades,
  createCombinedKlineSocket,
  toCoin,
  fromCoin,
};
