const path = require("path");
const { execFile } = require("child_process");
const config = require("./config");

const BRIDGE_PATH = path.join(__dirname, "hyperliquid_bridge.py");
const LOCAL_VENV_PYTHON = path.join(__dirname, ".venv", "bin", "python");

let requestGate = Promise.resolve();
let lastRequestAt = 0;
let resolvedPythonBinPromise = null;

function hyperliquidCoinFromPair(pair) {
  return String(pair || "")
    .trim()
    .toUpperCase()
    .replace(/USDT$/, "");
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isTransientError(error) {
  const message = String(error?.message || "").toLowerCase();
  return (
    message.includes("429") ||
    message.includes("timeout") ||
    message.includes("tempor") ||
    message.includes("connection") ||
    message.includes("reset") ||
    message.includes("econn") ||
    message.includes("rate limit")
  );
}

async function waitForRequestSlot(minGapMs = 300) {
  const run = async () => {
    const now = Date.now();
    const delay = Math.max(0, minGapMs - (now - lastRequestAt));
    if (delay > 0) {
      await sleep(delay);
    }
    lastRequestAt = Date.now();
  };

  const next = requestGate.then(run, run);
  requestGate = next.catch(() => {});
  return next;
}

function buildAuth(auth = {}) {
  return {
    secret_key: auth.secretKey || config.hyperliquidSecretKey || undefined,
    account_address:
      String(auth.accountAddress || config.hyperliquidAccountAddress || "").trim().toLowerCase() ||
      undefined,
    vault_address:
      String(auth.vaultAddress || config.hyperliquidVaultAddress || "").trim().toLowerCase() ||
      undefined,
  };
}

function candidatePythonBins() {
  return [...new Set(
    [
      config.hyperliquidPythonBin,
      LOCAL_VENV_PYTHON,
      "python3",
      "/opt/homebrew/bin/python3",
      "/Users/sumit/anaconda3/bin/python3",
      "python",
      "/Users/sumit/anaconda3/bin/python",
    ].filter(Boolean)
  )];
}

function rawBridgeCall(pythonBin, command, args = {}, auth = {}) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify({
      ...args,
      base_url: config.hyperliquidApiUrl,
      ...buildAuth(auth),
    });

    execFile(
      pythonBin,
      [BRIDGE_PATH, command, payload],
      {
        cwd: __dirname,
        env: {
          ...process.env,
          HYPERLIQUID_API_URL: config.hyperliquidApiUrl,
        },
        maxBuffer: 10 * 1024 * 1024,
      },
      (error, stdout, stderr) => {
        const raw = String(stdout || "").trim();
        let parsed = null;

        if (raw) {
          try {
            parsed = JSON.parse(raw);
          } catch (parseError) {
            reject(
              new Error(
                `Hyperliquid bridge returned invalid JSON via ${pythonBin}: ${parseError.message}\n${raw}\n${stderr || ""}`.trim()
              )
            );
            return;
          }
        }

        if (error) {
          const message =
            parsed?.error ||
            String(stderr || error.message || "Unknown Hyperliquid bridge failure").trim();
          reject(new Error(message));
          return;
        }

        if (!parsed?.ok) {
          reject(new Error(parsed?.error || "Hyperliquid bridge returned an error"));
          return;
        }

        resolve(parsed.data);
      }
    );
  });
}

async function resolvePythonBin(forceRefresh = false) {
  if (resolvedPythonBinPromise && !forceRefresh) {
    return resolvedPythonBinPromise;
  }

  resolvedPythonBinPromise = (async () => {
    const failures = [];

    for (const pythonBin of candidatePythonBins()) {
      try {
        const doctor = await rawBridgeCall(pythonBin, "doctor");
        if (doctor?.deps_ok) {
          return pythonBin;
        }
      } catch (error) {
        failures.push(`${pythonBin}: ${error.message}`);
      }
    }

    throw new Error(
      [
        "No working Python interpreter found for Hyperliquid bridge.",
        "Tried:",
        ...failures.map((line) => `- ${line}`),
        "Set HYPERLIQUID_PYTHON_BIN to an interpreter that has 'eth-account' and 'hyperliquid-python-sdk' installed.",
      ].join("\n")
    );
  })();

  try {
    return await resolvedPythonBinPromise;
  } catch (error) {
    resolvedPythonBinPromise = null;
    throw error;
  }
}

async function bridgeCall(command, args = {}, auth = {}) {
  const pythonBin = await resolvePythonBin();
  try {
    return await rawBridgeCall(pythonBin, command, args, auth);
  } catch (error) {
    if (/bridge-dependency-missing|No such file or directory|not found/i.test(String(error.message || ""))) {
      const fallbackPythonBin = await resolvePythonBin(true);
      return rawBridgeCall(fallbackPythonBin, command, args, auth);
    }
    throw error;
  }
}

async function callRead(command, args = {}, auth = {}, attempt = 1) {
  try {
    await waitForRequestSlot();
    return await bridgeCall(command, args, auth);
  } catch (error) {
    if (attempt < 4 && isTransientError(error)) {
      await sleep(Math.min(4_000, 300 * 2 ** attempt));
      return callRead(command, args, auth, attempt + 1);
    }
    throw error;
  }
}

async function callWrite(command, args = {}, auth = {}) {
  await waitForRequestSlot();
  return bridgeCall(command, args, auth);
}

function isConfigured(auth = {}) {
  return Boolean(buildAuth(auth).secret_key);
}

function hasUserContext(auth = {}) {
  const resolved = buildAuth(auth);
  return Boolean(resolved.account_address || resolved.vault_address || resolved.secret_key);
}

function getMeta() {
  return callRead("meta");
}

function getAllMids() {
  return callRead("all_mids");
}

function getUserState(auth = {}) {
  if (!hasUserContext(auth)) {
    throw new Error("invalid-user-profile");
  }
  return callRead("user_state", {}, auth);
}

function getSpotUserState(auth = {}) {
  if (!hasUserContext(auth)) {
    throw new Error("invalid-user-profile");
  }
  return callRead("spot_user_state", {}, auth);
}

function getOpenOrders(auth = {}) {
  if (!hasUserContext(auth)) {
    throw new Error("invalid-user-profile");
  }
  return callRead("open_orders", {}, auth);
}

function getOrderStatus({ oid, cloid }, auth = {}) {
  if (!hasUserContext(auth)) {
    throw new Error("invalid-user-profile");
  }
  return callRead(
    "order_status",
    {
      oid,
      cloid,
    },
    auth
  );
}

function getUserFillsByTime({ startTime, endTime, aggregateByTime = false }, auth = {}) {
  if (!hasUserContext(auth)) {
    throw new Error("invalid-user-profile");
  }
  return callRead(
    "user_fills_by_time",
    {
      start_time: startTime,
      end_time: endTime,
      aggregate_by_time: aggregateByTime,
    },
    auth
  );
}

function placeEntry({ coin, isBuy, size, leverage, slippage, cloid }, auth = {}) {
  if (!isConfigured(auth)) {
    throw new Error("secret-not-configured");
  }
  return callWrite(
    "place_entry",
    {
      coin,
      is_buy: isBuy,
      size,
      leverage,
      slippage,
      cloid,
    },
    auth
  );
}

function placeTpSl(
  { coin, isBuy, size, targetPx, stopPx, tpCloid, slCloid, grouping },
  auth = {}
) {
  if (!isConfigured(auth)) {
    throw new Error("secret-not-configured");
  }
  return callWrite(
    "place_tpsl",
    {
      coin,
      is_buy: isBuy,
      size,
      target_px: targetPx,
      stop_px: stopPx,
      tp_cloid: tpCloid,
      sl_cloid: slCloid,
      grouping,
    },
    auth
  );
}

function cancelOrders({ coin, oids }, auth = {}) {
  if (!isConfigured(auth)) {
    throw new Error("secret-not-configured");
  }
  return callWrite("cancel_orders", { coin, oids }, auth);
}

function marketClose({ coin, size, slippage, cloid }, auth = {}) {
  if (!isConfigured(auth)) {
    throw new Error("secret-not-configured");
  }
  return callWrite(
    "market_close",
    {
      coin,
      size,
      slippage,
      cloid,
    },
    auth
  );
}

function transferUsd({ amount, toPerp }, auth = {}) {
  if (!isConfigured(auth)) {
    throw new Error("secret-not-configured");
  }
  return callWrite(
    "usd_class_transfer",
    {
      amount,
      to_perp: toPerp,
    },
    auth
  );
}

module.exports = {
  hyperliquidCoinFromPair,
  isConfigured,
  hasUserContext,
  resolvePythonBin,
  getMeta,
  getAllMids,
  getUserState,
  getSpotUserState,
  getOpenOrders,
  getOrderStatus,
  getUserFillsByTime,
  placeEntry,
  placeTpSl,
  cancelOrders,
  marketClose,
  transferUsd,
};
