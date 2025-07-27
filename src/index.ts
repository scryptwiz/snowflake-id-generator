import os from "os";
import crypto from "crypto";

// Snowflake ID Generator - Twitter's algorithm for distributed unique IDs
// 64-bit structure: 1-bit sign | 41-bit timestamp | 10-bit machine | 12-bit sequence

export interface SnowflakeOptions {
  machineId?: number; // 0-1023, auto-generated from hostname if not provided
  epoch?: number; // Custom epoch timestamp, defaults to July 13, 2025
  warnOnDrift?: boolean; // Warn when clock drift is detected
}

export interface SnowflakeInfo {
  id: string;
  timestamp: number;
  machineId: number;
  sequence: number;
}

// Constants
const MACHINE_ID_BITS = 10;
const SEQUENCE_BITS = 12;
const MAX_MACHINE_ID = (1 << MACHINE_ID_BITS) - 1; // 1023
const MAX_SEQUENCE = (1 << SEQUENCE_BITS) - 1; // 4095
const MACHINE_ID_SHIFT = SEQUENCE_BITS; // 12
const TIMESTAMP_SHIFT = SEQUENCE_BITS + MACHINE_ID_BITS; // 22
const DEFAULT_EPOCH = 1752383159010; // July 13, 2025 06:05:59 GMT+01

// Helper Functions
function generateMachineIdFromHostname(): number {
  try {
    const hostname = os.hostname();
    const hash = crypto.createHash("sha256").update(hostname).digest();
    return ((hash[0] << 8) | hash[1]) & MAX_MACHINE_ID; // Use first two bytes for 10-bit machine ID
  } catch (error) {
    console.warn("Failed to generate machine ID from hostname, using random ID:", error);
    return Math.floor(Math.random() * (MAX_MACHINE_ID + 1));
  }
}

function validateMachineId(machineId: number): void {
  if (!Number.isInteger(machineId) || machineId < 0 || machineId > MAX_MACHINE_ID) {
    throw new Error(`Machine ID must be an integer between 0 and ${MAX_MACHINE_ID}`);
  }
}

function validateEpoch(epoch: number): void {
  const now = Date.now();
  if (epoch >= now) {
    throw new Error("Epoch must be in the past");
  }

  // Warn if close to 41-bit timestamp limit
  const maxTimestamp = (1n << 41n) - 1n;
  const timeUntilOverflow = Number(maxTimestamp - BigInt(now - epoch));
  const yearsUntilOverflow = timeUntilOverflow / (1000 * 60 * 60 * 24 * 365);

  if (yearsUntilOverflow < 1) {
    console.warn(`Snowflake timestamp will overflow in ${yearsUntilOverflow.toFixed(1)} years. Consider updating the epoch.`);
  }
}

function handleClockDrift(currentTimestamp: bigint, lastTimestamp: bigint, warnOnDrift: boolean): void {
  const drift = lastTimestamp - currentTimestamp;

  if (warnOnDrift) {
    console.warn(`[Snowflake] Clock moved backwards by ${drift}ms. Waiting for time to catch up.`);
  }

  // Throw error for significant drift (>5 seconds)
  if (drift > 5000n) {
    throw new Error(`Clock moved backwards by ${drift}ms. This may indicate a serious clock synchronization issue.`);
  }
}

function waitForNextMillisecond(lastTimestamp: bigint): bigint {
  let timestamp = BigInt(Date.now());
  while (timestamp <= lastTimestamp) {
    timestamp = BigInt(Date.now());
  }
  return timestamp;
}

function constructSnowflakeId(timestamp: bigint, epoch: bigint, machineId: number, sequence: number): bigint {
  return (
    ((timestamp - epoch) << BigInt(TIMESTAMP_SHIFT)) |
    (BigInt(machineId) << BigInt(MACHINE_ID_SHIFT)) |
    BigInt(sequence)
  );
}

// Generator state handler
function createGeneratorState() {
  let sequence = 0;
  let lastTimestamp = -1n;

  return {
    getSequence: () => sequence,
    setSequence: (newSequence: number) => { sequence = newSequence; },
    getLastTimestamp: () => lastTimestamp,
    setLastTimestamp: (timestamp: bigint) => { lastTimestamp = timestamp; },
    incrementSequence: () => { sequence = (sequence + 1) & MAX_SEQUENCE; },
    resetSequence: () => { sequence = 0; }
  };
}

function generateSnowflakeWithState(
  state: ReturnType<typeof createGeneratorState>,
  epoch: bigint,
  machineId: number,
  warnOnDrift: boolean
): { id: bigint; timestamp: bigint; sequence: number } {
  let timestamp = BigInt(Date.now());

  // Handle clock drift
  if (timestamp < state.getLastTimestamp()) {
    handleClockDrift(timestamp, state.getLastTimestamp(), warnOnDrift);
    timestamp = waitForNextMillisecond(state.getLastTimestamp());
  }

  // Handle sequence within same millisecond
  if (timestamp === state.getLastTimestamp()) {
    state.incrementSequence();
    if (state.getSequence() === 0) {
      timestamp = waitForNextMillisecond(state.getLastTimestamp()); // Sequence overflow, wait for next ms
    }
  } else {
    state.resetSequence();
  }

  state.setLastTimestamp(timestamp);
  const id = constructSnowflakeId(timestamp, epoch, machineId, state.getSequence());
  
  return { id, timestamp, sequence: state.getSequence() };
}

export function createSnowflakeGenerator(options: SnowflakeOptions = {}) {
  // Setup configuration
  const machineId = options.machineId ?? generateMachineIdFromHostname();
  const epoch = BigInt(options.epoch ?? DEFAULT_EPOCH);
  const warnOnDrift = options.warnOnDrift ?? true;

  // Validate configuration
  if (options.machineId !== undefined) {
    validateMachineId(machineId);
  }
  validateEpoch(Number(epoch));

  const state = createGeneratorState();

  function generate(): string {
    const result = generateSnowflakeWithState(state, epoch, machineId, warnOnDrift);
    return result.id.toString();
  }

  function generateWithInfo(): SnowflakeInfo {
    const result = generateSnowflakeWithState(state, epoch, machineId, warnOnDrift);
    return {
      id: result.id.toString(),
      timestamp: Number(result.timestamp),
      machineId,
      sequence: result.sequence,
    };
  }

  function getMachineId(): number {
    return machineId;
  }

  function getEpoch(): number {
    return Number(epoch);
  }

  // Attach methods to generate function
  const generator = generate as typeof generate & {
    generateWithInfo: typeof generateWithInfo;
    getMachineId: typeof getMachineId;
    getEpoch: typeof getEpoch;
  };

  generator.generateWithInfo = generateWithInfo;
  generator.getMachineId = getMachineId;
  generator.getEpoch = getEpoch;

  return generator;
}

export function parseSnowflakeId(id: string | bigint, epoch: number = DEFAULT_EPOCH): SnowflakeInfo {
  const idBigInt = typeof id === "string" ? BigInt(id) : id;

  const sequence = Number(idBigInt & BigInt(MAX_SEQUENCE));
  const machineId = Number((idBigInt >> BigInt(MACHINE_ID_SHIFT)) & BigInt(MAX_MACHINE_ID));
  const timestamp = Number((idBigInt >> BigInt(TIMESTAMP_SHIFT)) + BigInt(epoch));

  return { id: id.toString(), timestamp, machineId, sequence };
}

// Default generator instance - uses auto-generated machine ID from hostname
export const defaultGenerator = createSnowflakeGenerator();

// Convenience function to generate a single Snowflake ID
export function generateSnowflakeId(options?: SnowflakeOptions): string {
  if (!options) {
    return defaultGenerator();
  }

  const generator = createSnowflakeGenerator(options);
  return generator();
}
