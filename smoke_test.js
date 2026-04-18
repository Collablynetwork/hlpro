const fs = require("fs");
const path = require("path");
const assert = require("assert");

const projectDir = __dirname;
const storageDir = path.join(projectDir, "storage");
if (fs.existsSync(storageDir)) fs.rmSync(storageDir, { recursive: true, force: true });

const state = require("./state");
state.ensureStorage();

const signals = require("./signals");
const tradeManager = require("./dryrun");
const pairUniverse = require("./pairUniverse");
const hyperliquid = require("./hyperliquid");

pairUniverse.validatePair = async (pair) => ({
  ok: true,
  pair: String(pair).toUpperCase(),
  coin: String(pair).toUpperCase().replace(/USDT$/, ""),
  meta: { szDecimals: 3 },
  link: `https://app.hyperliquid.xyz/trade/${String(pair).toUpperCase().replace(/USDT$/, "")}`,
});
hyperliquid.getAllMids = async () => ({ BTC: 100 });

function makeMatch(overrides = {}) {
  return {
    pair: "BTCUSDT",
    side: "LONG",
    score: 88,
    baseTimeframe: "1m",
    entry: 100,
    entryPrice: 100,
    currentPrice: 100,
    sl: 99.2,
    tp1: 100.8,
    tp2: 101.6,
    tp3: 102.4,
    supportTimeframes: ["1m", "5m", "15m"],
    reasons: ["rule A", "rule B"],
    ...overrides,
  };
}

const profile = state.getProfileById(state.DEFAULT_PROFILE_ID);

assert(profile, "Default profile should exist");
assert.deepStrictEqual(
  state.getWatchedPairs(profile.id),
  ["BNBUSDT", "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"].sort()
);

const runtime = state.getRuntimeSettings(profile.id);
assert.strictEqual(runtime.tradeBalanceTarget, 100);
assert.strictEqual(runtime.tradeLeverage, 10);
assert.strictEqual(runtime.capitalMode, "SIMPLE");
assert.strictEqual(runtime.executionMode, "DEMO");
assert.strictEqual(runtime.baselinePrincipal, 100);
assert.strictEqual(runtime.simpleSlots, 1);

state.saveWatchedPairs(profile.id, ["DOGEUSDT"]);
assert.deepStrictEqual(state.getWatchedPairs(profile.id), ["DOGEUSDT"]);

assert(signals.buildSignalCandidate(makeMatch({ baseTimeframe: "15m" })), "15m should be allowed");
assert(signals.buildSignalCandidate(makeMatch({ supportTimeframes: ["1m", "5m"] })), "Support count should not hard-block");

const candidate = signals.buildSignalCandidate(makeMatch());
assert(candidate, "Signal candidate should pass old gate");
assert.strictEqual(candidate.entryPrice.toFixed(4), "100.0000");
assert.strictEqual(candidate.tp1.toFixed(4), "100.8000");
assert.strictEqual(candidate.tp2.toFixed(4), "101.6000");
assert.strictEqual(candidate.tp3.toFixed(4), "102.4000");
assert.strictEqual(candidate.stopLoss.toFixed(4), "99.2000");

let allocation = tradeManager.buildAllocationPlan(candidate, profile.id);
assert.strictEqual(allocation.accept, true);
assert.strictEqual(allocation.allocationPct, 1);

state.savePairState(profile.id, "BTCUSDT", {
  pair: "BTCUSDT",
  lastClosedAt: new Date(Date.now() - 15 * 60 * 1000).toISOString(),
  lastSide: "LONG",
  lastEntryPrice: 100,
  repeatLevel: 0,
  cooldownUntil: null,
});
allocation = tradeManager.buildAllocationPlan({ ...candidate, entryPrice: 101, entry: 101 }, profile.id);
assert.strictEqual(allocation.accept, true);
assert.strictEqual(allocation.allocationPct, 0.25);
assert.strictEqual(allocation.repeatLevel, 1);

tradeManager
  .registerSignal(candidate, { profileId: profile.id, signalMessageId: 111 })
  .then((result) => {
    assert(result.trade, "Demo signal should create a trade");
    assert.strictEqual(result.trade.executionMode, "DEMO");
    assert.strictEqual(result.trade.capitalMode, "SIMPLE");
    assert.strictEqual(result.events.some((event) => event.type === "ENTRY_FILLED"), true);

    const lockState = state.getModeLockState(profile.id);
    assert.strictEqual(lockState.locked, true);

    const sampleStrategy = {
      id: "BTCUSDT-long-1m-1",
      pair: "BTCUSDT",
      direction: "long",
      eventTime: new Date().toISOString(),
      fileName: "sample.json",
      mainSourceTimeframe: "1m",
      savedTimeframes: ["1m", "5m"],
      supportingTimeframes: ["1m", "5m", "15m"],
      resultingExpansionPct: 12.5,
      triggerFeatures: {},
      flowFeatures: {},
      allTimeframes: {},
    };

    state.saveStrategy(sampleStrategy);
    const exported = state.exportStrategiesText();
    const imported = state.importStrategiesText(exported, { replace: true });
    assert.strictEqual(imported.strategies.length, 1);
    assert.strictEqual(imported.strategies[0].id, sampleStrategy.id);

    console.log("All smoke tests passed");
    console.log(
      JSON.stringify(
        {
          candidate,
          runtime,
          allocation,
          strategies: state.loadStrategiesIndex().length,
          openTrades: state.listOpenTrades(profile.id).length,
        },
        null,
        2
      )
    );
  })
  .catch((error) => {
    console.error(error);
    process.exit(1);
  });
