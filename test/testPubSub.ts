import { Attributes, PubSub } from '@google-cloud/pubsub';

const topic = 'insert-to-bq';
const datasetId = 'dataset';
const tablePrefix = 'test';

const testData = {
  answerId: 'AAAA',
  timestamp: {
    _seconds: 1576792986,
    _nanoseconds: 123456789
  },
  A1: 'AA',
  A2: 'BB',
  A3: ['CC', 'DD', 'EE'],
  Score1: 3.1415926,
  Score2: 7
};

main().catch(console.error);

async function main() {
  const pubsub = new PubSub();
  const data = Buffer.from(JSON.stringify(testData));
  const attr: Attributes = { datasetId, tablePrefix };
  const res = await pubsub.topic(topic).publish(data, attr);
  console.log(res);
}
