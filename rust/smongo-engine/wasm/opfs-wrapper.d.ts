/** Stable machine-readable codes for `OpfsError` / monitoring. */
export const OPFS_ERROR_CODES: Readonly<{
  INVALID_DB_NAME: 'OPFS_INVALID_DB_NAME';
  INVALID_COLLECTION: 'OPFS_INVALID_COLLECTION';
  INVALID_PAYLOAD: 'OPFS_INVALID_PAYLOAD';
  INVALID_REQUEST: 'OPFS_INVALID_REQUEST';
  RPC_UNKNOWN_OP: 'OPFS_RPC_UNKNOWN_OP';
  RPC_TIMEOUT: 'OPFS_RPC_TIMEOUT';
  RPC_TOO_MANY_IN_FLIGHT: 'OPFS_RPC_TOO_MANY_IN_FLIGHT';
  OWNER_LOST: 'OPFS_OWNER_LOST';
  RECONNECTING: 'OPFS_RECONNECTING';
  WORKER_ERROR: 'OPFS_WORKER_ERROR';
  WORKER_MESSAGE_TIMEOUT: 'OPFS_WORKER_MESSAGE_TIMEOUT';
  NOT_INITIALIZED: 'OPFS_NOT_INITIALIZED';
  ALREADY_OPEN_ELSEWHERE: 'OPFS_ALREADY_OPEN_ELSEWHERE';
  OWNER_UNAVAILABLE: 'OPFS_OWNER_UNAVAILABLE';
  ALREADY_INITIALIZED: 'OPFS_ALREADY_INITIALIZED';
}>;

export const OPFS_RPC_LIMITS: Readonly<{
  maxDbNameLength: number;
  maxCollectionNameLength: number;
  maxRequestIdLength: number;
  maxPayloadWeight: number;
  maxObjectDepth: number;
  maxClientPendingRpc: number;
  defaultTimeoutMs: number;
  pingTimeoutMs: number;
  pingAttempts: number;
  pingBackoffMs: number;
  workerMessageTimeoutMs: number;
}>;

export class OpfsError extends Error {
  readonly name: 'OpfsError';
  /** Stable string equal to one of `Object.values(OPFS_ERROR_CODES)`. */
  readonly code: string;
  cause?: unknown;
  constructor(message: string, code: string, opts?: { cause?: unknown });
}

export function isOpfsError(e: unknown): e is OpfsError;

export function configureOpfsDebug(opts?: { enabled?: boolean }): void;

export function assertValidDbName(name: unknown): string;

export function initOpfsDatabase(dbName: string, collections?: string[]): Promise<OpfsDatabase>;

export function reconnectOpfsDatabase(dbName: string, collections?: string[]): Promise<OpfsDatabase>;

export function closeOpfsDatabase(dbName: string): Promise<void>;

export function wipeOpfsDatabaseDirectory(dbName: string): Promise<void>;

/** BSON-shaped results from the engine; narrow in application code as needed. */
export type OpfsBsonDoc = Record<string, unknown>;

export class OpfsDatabase {
  constructor(dbName: string, options?: { mode?: 'owner' | 'client' });
  collection(name: string): OpfsCollection;
}

export class OpfsCollection {
  insertOne(doc: OpfsBsonDoc): Promise<OpfsBsonDoc>;
  find(filter?: OpfsBsonDoc): Promise<OpfsBsonDoc[]>;
  countDocuments(filter?: OpfsBsonDoc): Promise<number>;
  deleteMany(filter: OpfsBsonDoc): Promise<OpfsBsonDoc>;
  updateMany(filter: OpfsBsonDoc, update: OpfsBsonDoc): Promise<OpfsBsonDoc>;
}
