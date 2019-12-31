import { insertToBq } from './insertToBq';

interface PubsubMessage {
  data: string;
  attributes: {[k: string]: string};
  // ...
}

interface PubsubContext {
  eventId: string;
  timestamp: string;
  // ...
}

export async function subInsertBq(msg: PubsubMessage, context: PubsubContext) {
  try {
    const eventAge = Date.now() - Date.parse(context.timestamp);
    if (eventAge > 30 * 60 * 1000) {
      throw new Error(`dropping event of age ${eventAge} ms`);
    }

    const pubSubData = msg.data;
    const datasetId = msg.attributes.datasetId;
    const tablePrefix = msg.attributes.tablePrefix;
    const messageId = context.eventId;
    const buffer = Buffer.from(pubSubData, 'base64');
    const data = JSON.parse(buffer.toString());

    const isReady = await insertToBq(datasetId, tablePrefix, data, messageId);
    if (!isReady) {
      console.log(`NOT READY :: let function timeout and retry`);
      await new Promise(r => setTimeout(r, 10 * 60 * 1000)); // "never" <- GCF max timeout is 9 minutes
    }
  } catch (e) {
    console.error(`ABORT :: ${e.message}`);
  }
}
