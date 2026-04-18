const config = require("./config");

function num(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function bool(value) {
  return Boolean(value);
}

function clamp(value, min = 0, max = 100) {
  return Math.max(min, Math.min(max, value));
}

function round(value, digits = 4) {
  const n = num(value, null);
  if (n === null) return null;
  const p = 10 ** digits;
  return Math.round(n * p) / p;
}

function getFeature(tfMap, timeframe) {
  return tfMap?.[timeframe]?.features || {};
}

function getFlow(tfMap, timeframe) {
  return tfMap?.[timeframe]?.flow || {};
}

function getStrategyMainTf(strategy) {
  return (
    strategy?.mainSourceTimeframe ||
    strategy?.fingerprint?.timeframe ||
    strategy?.sourceTimeframes?.[0] ||
    "15m"
  );
}

function getStrategyTrigger(strategy) {
  return strategy?.triggerFeatures || strategy?.fingerprint?.features || {};
}

function getStrategyFlow(strategy) {
  return strategy?.flowFeatures || strategy?.fingerprint?.flow || {};
}

function findSupportingTimeframes(tfMap, baseTimeframe, side) {
  const hierarchy = config.timeframeHierarchyMap || {};
  const tfs = hierarchy[baseTimeframe] || [baseTimeframe];
  const out = [];

  for (const tf of tfs) {
    const features = getFeature(tfMap, tf);
    if (!Object.keys(features).length) continue;

    if (side === "LONG") {
      const ok =
        features.trend === "uptrend" ||
        bool(features.bullishBos) ||
        num(features.diSpread, 0) > 0 ||
        (num(features.macdLine, 0) >= num(features.macdSignal, 0) &&
          num(features.macdHistogramSlope, 0) >= 0);

      if (ok) out.push(tf);
    } else {
      const ok =
        features.trend === "downtrend" ||
        bool(features.bearishBos) ||
        num(features.diSpread, 0) < 0 ||
        (num(features.macdLine, 0) <= num(features.macdSignal, 0) &&
          num(features.macdHistogramSlope, 0) <= 0);

      if (ok) out.push(tf);
    }
  }

  return [...new Set(out)];
}

function chooseBestBaseTimeframes(strategyTf) {
  const hierarchy = config.timeframeHierarchyMap || {};
  return hierarchy[strategyTf] || [strategyTf];
}

function closenessScore(current, target, tolerancePct = 30) {
  const c = num(current, null);
  const t = num(target, null);

  if (c === null || t === null) return null;
  if (Math.abs(t) < 1e-12) {
    return Math.abs(c) < 1e-12 ? 1 : Math.max(0, 1 - Math.abs(c));
  }

  const diffPct = (Math.abs(c - t) / Math.abs(t)) * 100;
  return clamp(1 - diffPct / tolerancePct, 0, 1);
}

function sideStateOk(side, features, flow) {
  if (side === "LONG") {
    return {
      macdAligned:
        num(features.macdLine, 0) >= num(features.macdSignal, 0) ||
        bool(features.macdBullCross),
      histAligned: num(features.macdHistogramSlope, 0) >= 0,
      bosAligned: bool(features.bullishBos),
      trendAligned:
        features.trend === "uptrend" || num(features.diSpread, 0) > 0,
      takerAligned: num(flow.takerBuySellRatio, 1) >= 1,
      oiAligned: num(flow.openInterestChangePct, 0) >= 0,
    };
  }

  return {
    macdAligned:
      num(features.macdLine, 0) <= num(features.macdSignal, 0) ||
      bool(features.macdBearCross),
    histAligned: num(features.macdHistogramSlope, 0) <= 0,
    bosAligned: bool(features.bearishBos),
    trendAligned:
      features.trend === "downtrend" || num(features.diSpread, 0) < 0,
    takerAligned: num(flow.takerBuySellRatio, 1) <= 1,
    oiAligned: num(flow.openInterestChangePct, 0) <= 0,
  };
}

function computeSimilarity(strategy, tfMap, baseTimeframe, side) {
  const features = getFeature(tfMap, baseTimeframe);
  const flow = getFlow(tfMap, baseTimeframe);
  const strategyTrigger = getStrategyTrigger(strategy);
  const strategyFlow = getStrategyFlow(strategy);
  const weights = strategy?.learnedWeights || {};

  if (!Object.keys(features).length) return null;

  const state = sideStateOk(side, features, flow);
  const supportTfs = findSupportingTimeframes(tfMap, baseTimeframe, side);

  const w = {
    bb: num(weights.bb, 14),
    macdZero: num(weights.macdZero, 12),
    macdCross: num(weights.macdCross, 12),
    hist: num(weights.hist ?? weights.histogram, 10),
    adx: num(weights.adx, 10),
    structure: num(weights.structure, 14),
    volume: num(weights.volume, 10),
    oi: num(weights.oi, 7),
    taker: num(weights.taker, 6),
    support: num(weights.support, 5),
  };

  const scores = [];
  const reasons = [];

  const bbScore = closenessScore(
    features.bbWidthPercentile,
    strategyTrigger.bbWidthPercentile,
    50
  );
  if (bbScore !== null) {
    scores.push(bbScore * w.bb);
    if (bbScore > 0.55) {
      reasons.push(
        `BB compression aligned (${round(features.bbWidthPercentile, 4)})`
      );
    }
  }

  const currentMacdZeroDistance = Math.abs(num(features.macdLine, 0));
  const targetMacdZeroDistance = Math.abs(
    num(strategyTrigger.macdLine, 0)
  );
  const macdZeroScore = closenessScore(
    currentMacdZeroDistance,
    targetMacdZeroDistance,
    60
  );
  if (macdZeroScore !== null) {
    scores.push(macdZeroScore * w.macdZero);
    if (macdZeroScore > 0.5) {
      reasons.push("MACD zero-line approach aligned");
    }
  }

  const macdCrossScore = state.macdAligned ? 1 : 0;
  scores.push(macdCrossScore * w.macdCross);
  if (macdCrossScore) {
    reasons.push(
      side === "LONG"
        ? "MACD bullish alignment matched"
        : "MACD bearish alignment matched"
    );
  }

  const histTarget = num(strategyTrigger.macdHistogramSlope, 0);
  const histCurrent = num(features.macdHistogramSlope, 0);
  const histScore =
    side === "LONG"
      ? histCurrent >= Math.min(histTarget, 0)
        ? 1
        : 0
      : histCurrent <= Math.max(histTarget, 0)
      ? 1
      : 0;

  scores.push(histScore * w.hist);
  if (histScore) reasons.push("Histogram slope aligned");

  const adxTarget = num(strategyTrigger.adx, 18);
  const adxCurrent = num(features.adx, 0);
  const adxSlope = num(features.adxSlope, 0);
  const adxScore =
    adxCurrent >= Math.max(14, adxTarget * 0.7) && adxSlope >= 0 ? 1 : 0;
  scores.push(adxScore * w.adx);
  if (adxScore) reasons.push(`ADX pressure aligned (${round(adxCurrent, 4)})`);

  const structureScore =
    side === "LONG"
      ? state.bosAligned || state.trendAligned
        ? 1
        : 0
      : state.bosAligned || state.trendAligned
      ? 1
      : 0;

  scores.push(structureScore * w.structure);
  if (structureScore) reasons.push("Structure and BOS aligned");

  const volumeTarget = Math.max(1, num(strategyTrigger.volumeVsAvg20, 1));
  const volumeCurrent = num(features.volumeVsAvg20, 0);
  const volumeScore = volumeCurrent >= volumeTarget * 0.5 ? 1 : 0;
  scores.push(volumeScore * w.volume);
  if (volumeScore) {
    reasons.push(`Volume/avg20 aligned (${round(volumeCurrent, 4)})`);
  }

  const oiTarget = num(strategyFlow.openInterestChangePct, 0);
  const oiCurrent = num(flow.openInterestChangePct, 0);
  let oiScore = 0;
  if (side === "LONG") {
    oiScore = oiCurrent >= Math.min(oiTarget, 0) ? 1 : 0;
  } else {
    oiScore = oiCurrent <= Math.max(oiTarget, 0) ? 1 : 0;
  }
  if (!state.oiAligned) oiScore *= 0.6;
  scores.push(oiScore * w.oi);
  if (oiScore > 0.5) {
    reasons.push(`OI change aligned (${round(oiCurrent, 4)}%)`);
  }

  const takerTarget = num(strategyFlow.takerBuySellRatio, 1);
  const takerCurrent = num(flow.takerBuySellRatio, 1);
  let takerScore = 0;
  if (side === "LONG") {
    takerScore = takerCurrent >= Math.min(1, takerTarget) ? 1 : 0;
  } else {
    takerScore = takerCurrent <= Math.max(1, takerTarget) ? 1 : 0;
  }
  if (!state.takerAligned) takerScore *= 0.6;
  scores.push(takerScore * w.taker);
  if (takerScore > 0.5) {
    reasons.push(`Taker ratio aligned (${round(takerCurrent, 4)})`);
  }

  const supportScore = clamp(supportTfs.length / 4, 0, 1);
  scores.push(supportScore * w.support);
  if (supportScore > 0) {
    reasons.push(`Higher timeframe support: ${supportTfs.join(", ")}`);
  }

  const totalWeight = Object.values(w).reduce((a, b) => a + b, 0);
  const rawScore = scores.reduce((a, b) => a + b, 0);
  const similarityScore = clamp((rawScore / totalWeight) * 100, 0, 100);

  const currentPrice =
    num(features.currentClose, null) ??
    num(features.close, null);

  if (currentPrice === null || currentPrice <= 0) {
    return null;
  }

  const support = num(features.support, null);
  const resistance = num(features.resistance, null);
  const bbLower = num(features.bbLower, null);
  const bbUpper = num(features.bbUpper, null);

  let stopLoss = null;
  let tp1 = null;
  let tp2 = null;
  let tp3 = null;

  if (side === "LONG") {
    stopLoss =
      support && support < currentPrice
        ? support
        : bbLower && bbLower < currentPrice
        ? bbLower
        : currentPrice * 0.985;

    const risk = Math.max(currentPrice - stopLoss, currentPrice * 0.004);
    tp1 = currentPrice + risk;
    tp2 = currentPrice + risk * 2;
    tp3 =
      resistance && resistance > tp2
        ? resistance
        : currentPrice + risk * 3;
  } else {
    stopLoss =
      resistance && resistance > currentPrice
        ? resistance
        : bbUpper && bbUpper > currentPrice
        ? bbUpper
        : currentPrice * 1.015;

    const risk = Math.max(stopLoss - currentPrice, currentPrice * 0.004);
    tp1 = currentPrice - risk;
    tp2 = currentPrice - risk * 2;
    tp3 =
      support && support < tp2
        ? support
        : currentPrice - risk * 3;
  }

  const rr =
    stopLoss !== null && tp1 !== null
      ? Math.abs(tp1 - currentPrice) /
        Math.max(Math.abs(currentPrice - stopLoss), 1e-12)
      : null;

  return {
    pair: null,
    side,
    score: round(similarityScore, 2),
    similarityScore: round(similarityScore, 2),

    entry: round(currentPrice, 8),
    entryPrice: round(currentPrice, 8),
    currentPrice: round(currentPrice, 8),

    sl: round(stopLoss, 8),
    stopLoss: round(stopLoss, 8),
    tp1: round(tp1, 8),
    tp2: round(tp2, 8),
    tp3: round(tp3, 8),

    baseTimeframe,
    baseTf: baseTimeframe,
    supportTfs,

    reasons,
    riskReward: rr ? round(rr, 2) : null,
    regimeSupportScore: round(supportScore * 100, 2),

    strategySourcePair: strategy?.pair || "N/A",
    strategySourceTimeframe: getStrategyMainTf(strategy),

    strategyId: strategy?.id || null,
  };
}

function bestMatchForSide(pair, tfMap, strategies, side) {
  const filteredStrategies = (strategies || []).filter(
    (strategy) =>
      String(strategy?.direction || "").toUpperCase() ===
      (side === "LONG" ? "LONG" : "SHORT")
  );

  if (!filteredStrategies.length) return null;

  let best = null;

  for (const strategy of filteredStrategies) {
    const sourceTf = getStrategyMainTf(strategy);
    const candidateBaseTfs = chooseBestBaseTimeframes(sourceTf);

    for (const timeframe of candidateBaseTfs) {
      if (!tfMap?.[timeframe]) continue;

      const candidate = computeSimilarity(strategy, tfMap, timeframe, side);
      if (!candidate) continue;

      candidate.pair = pair;

      if (!best || num(candidate.score, 0) > num(best.score, 0)) {
        best = candidate;
      }
    }
  }

  return best;
}

function matchStrategiesForPair(pair, tfMap, strategies) {
  return {
    long: bestMatchForSide(pair, tfMap, strategies, "LONG"),
    short: bestMatchForSide(pair, tfMap, strategies, "SHORT"),
  };
}

module.exports = {
  matchStrategiesForPair,
};
