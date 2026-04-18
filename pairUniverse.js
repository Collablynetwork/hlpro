const binance = require("./binance");
const config = require("./config");
const state = require("./state");

let memoryCache = {
  value: null,
  expiresAt: 0,
};

function now() {
  return Date.now();
}

function normalizePair(pair) {
  return String(pair || "").trim().toUpperCase();
}

function coinFromPair(pair) {
  return normalizePair(pair).replace(/USDT$/, "");
}

function pairFromCoin(coin) {
  return `${String(coin || "").trim().toUpperCase()}USDT`;
}

function buildHyperliquidLink(pairOrCoin) {
  const raw = normalizePair(pairOrCoin);
  const coin = raw.endsWith("USDT") ? coinFromPair(raw) : raw;
  return `https://app.hyperliquid.xyz/trade/${coin}`;
}

function formatIsValidPair(pair) {
  return /^[A-Z0-9]+USDT$/.test(normalizePair(pair));
}

function toSnapshotPayload(exchangeInfo) {
  const symbols = Array.isArray(exchangeInfo?.symbols) ? exchangeInfo.symbols : [];
  const pairs = [];
  const aliasToCoin = {};
  const coinMeta = {};

  for (const symbol of symbols) {
    const pair = normalizePair(symbol.symbol);
    const coin = normalizePair(symbol.baseAsset);
    if (!pair || !coin) continue;
    pairs.push(pair);
    aliasToCoin[pair] = coin;
    coinMeta[coin] = {
      pair,
      coin,
      maxLeverage: Number(symbol.maxLeverage || 0),
      szDecimals: Number(symbol.szDecimals || 0),
      onlyIsolated: Boolean(symbol.onlyIsolated),
      status: symbol.status || "TRADING",
    };
  }

  return {
    pairs,
    aliasToCoin,
    coinMeta,
    fetchedAt: new Date().toISOString(),
  };
}

async function refreshMetadata(options = {}) {
  const exchangeInfo = await binance.getExchangeInfo();
  const payload = toSnapshotPayload(exchangeInfo);
  memoryCache = {
    value: payload,
    expiresAt: now() + Number(config.hyperliquidMetaCacheMs || 10 * 60 * 1000),
  };
  state.setSnapshot("hyperliquidMetaCache", payload);
  return payload;
}

async function getMetadata(options = {}) {
  const force = Boolean(options.force);
  if (!force && memoryCache.value && memoryCache.expiresAt > now()) {
    return memoryCache.value;
  }

  const cached = state.getSnapshot("hyperliquidMetaCache", null);
  if (!force && cached?.pairs?.length) {
    memoryCache = {
      value: cached,
      expiresAt: now() + Number(config.hyperliquidMetaCacheMs || 10 * 60 * 1000),
    };
    return cached;
  }

  try {
    return await refreshMetadata(options);
  } catch (error) {
    if (cached?.pairs?.length) {
      memoryCache = {
        value: cached,
        expiresAt: now() + 60_000,
      };
      return cached;
    }
    throw error;
  }
}

async function getTradablePairSet(options = {}) {
  const metadata = await getMetadata(options);
  return new Set((metadata?.pairs || []).map(normalizePair));
}

async function getCoinMetaByPair(pair, options = {}) {
  const normalized = normalizePair(pair);
  const metadata = await getMetadata(options);
  const coin = metadata?.aliasToCoin?.[normalized] || coinFromPair(normalized);
  return metadata?.coinMeta?.[coin] || null;
}

async function validatePair(pair, options = {}) {
  const normalized = normalizePair(pair);
  if (!formatIsValidPair(normalized)) {
    return {
      ok: false,
      pair: normalized,
      reason: "invalid-pair",
    };
  }

  const metadata = await getMetadata(options);
  const coin = metadata?.aliasToCoin?.[normalized] || coinFromPair(normalized);
  const meta = metadata?.coinMeta?.[coin] || null;

  if (!meta) {
    return {
      ok: false,
      pair: normalized,
      coin,
      reason: "not-listed-on-hyperliquid",
    };
  }

  return {
    ok: true,
    pair: normalized,
    coin,
    meta,
    link: buildHyperliquidLink(coin),
  };
}

module.exports = {
  normalizePair,
  formatIsValidPair,
  pairFromCoin,
  coinFromPair,
  buildHyperliquidLink,
  getMetadata,
  refreshMetadata,
  getTradablePairSet,
  getCoinMetaByPair,
  validatePair,
};
