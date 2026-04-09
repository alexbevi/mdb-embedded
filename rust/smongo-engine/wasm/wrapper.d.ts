export function initSmongo(): Promise<void>;

export type BsonDoc = Record<string, unknown>;

export class Database {
  constructor(name: string);
  collection(name: string): Collection;
}

export class Collection {
  insertOne(doc: BsonDoc): BsonDoc;
  find(filter?: BsonDoc): BsonDoc[];
  countDocuments(filter?: BsonDoc): number;
  deleteMany(filter: BsonDoc): BsonDoc;
  updateMany(filter: BsonDoc, update: BsonDoc): BsonDoc;
}
