const crypto = require("crypto");
const config = require("./config");

function buildKey() {
  return crypto
    .createHash("sha256")
    .update(String(config.stateEncryptionKey || ""))
    .digest();
}

function fingerprintSecret(secret) {
  const digest = crypto
    .createHash("sha256")
    .update(String(secret || ""))
    .digest("hex");
  return `${digest.slice(0, 8)}...${digest.slice(-6)}`;
}

function encryptSecret(secret) {
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv("aes-256-gcm", buildKey(), iv);
  const encrypted = Buffer.concat([
    cipher.update(String(secret || ""), "utf8"),
    cipher.final(),
  ]);
  const tag = cipher.getAuthTag();
  return Buffer.concat([iv, tag, encrypted]).toString("base64");
}

function decryptSecret(payload) {
  if (!payload) return "";
  const buffer = Buffer.from(String(payload), "base64");
  const iv = buffer.subarray(0, 12);
  const tag = buffer.subarray(12, 28);
  const encrypted = buffer.subarray(28);
  const decipher = crypto.createDecipheriv("aes-256-gcm", buildKey(), iv);
  decipher.setAuthTag(tag);
  return Buffer.concat([decipher.update(encrypted), decipher.final()]).toString("utf8");
}

function maskAddress(address) {
  const raw = String(address || "").trim();
  if (!raw) return "not-set";
  if (raw.length <= 12) return raw;
  return `${raw.slice(0, 6)}...${raw.slice(-4)}`;
}

module.exports = {
  fingerprintSecret,
  encryptSecret,
  decryptSecret,
  maskAddress,
};
