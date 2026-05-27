import { MongoClient } from 'mongodb';

const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017';
const MONGO_DB = process.env.MONGO_DB || 'hm_recsys';

let client = null;
let db = null;

export async function connectMongo() {
  if (db) return db;
  client = new MongoClient(MONGO_URI, {
    serverSelectionTimeoutMS: 5000,
  });
  await client.connect();
  db = client.db(MONGO_DB);
  console.log(`[mongo] connected to ${MONGO_URI} db=${MONGO_DB}`);
  return db;
}

export function getDb() {
  if (!db) throw new Error('Mongo not connected yet — call connectMongo() first');
  return db;
}

export async function closeMongo() {
  if (client) await client.close();
  client = null;
  db = null;
}
