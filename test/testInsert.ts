import { insertToBq } from '../src/insertToBq';

async function main() {
  const datasetId = 'dataset';
  const tablePrefix = 'table';
  const data: any = {
    age: [30, 21, 33],
    foo: 123,
    name: 'Tom',
    timestampValue: [
      { _seconds: 1576692986, _nanoseconds: 123456789 },
      { _seconds: 1576692986, _nanoseconds: 123456789 }
    ],
    var: 123.222,
  };
  await insertToBq(datasetId, tablePrefix, data);
}

main().catch(console.error);
