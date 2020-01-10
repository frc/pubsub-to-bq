// Import the Google Cloud client library
import { BigQuery, TableField } from '@google-cloud/bigquery';
import { Timestamp } from '@google-cloud/firestore';
import { createHash } from 'crypto';
import { ok, notStrictEqual, deepStrictEqual } from 'assert';

const isString = (v: unknown): v is string => typeof v === 'string';
const isBoolean = (v: unknown): v is boolean => typeof v === 'boolean';
const isNumber = (v: unknown): v is number => typeof v === 'number';
const isArray = (v: unknown): v is unknown[] => Array.isArray(v);
const isRecord = (v: unknown): v is object => typeof v === 'object' && v !== null;
const isTimestamp = (v: unknown): v is {_seconds: number, _nanoseconds: number} =>
  typeof v === 'object' && v !== null && Object.keys(v).length === 2 &&
  Object.keys(v).includes('_seconds') && Object.keys(v).includes('_nanoseconds');

function nameValueToSchemaValueTuple(name: string, value: unknown): [TableField, unknown] {
  let fieldSchema: TableField = { name, mode: 'REQUIRED' };
  let fieldValue: unknown = value;

  if (isString(value)) {
    fieldSchema.type = 'STRING';
  } else if (isBoolean(value)) {
    fieldSchema.type = 'BOOLEAN';
  } else if (isNumber(value)) {
    fieldSchema.type = 'FLOAT';
  } else if (isTimestamp(value)) {
    fieldSchema.type = 'TIMESTAMP';
    const ts = new Timestamp(value._seconds, value._nanoseconds);
    fieldValue = ts.toDate().toISOString().replace(/T/, ' ').replace(/\.\d+Z$/, '.' + ts.nanoseconds);
  } else if (isArray(value)) {
    const entries = value.map(v => nameValueToSchemaValueTuple(name, v));
    const schemata = entries.map(c => c[0]);
    notStrictEqual(schemata.length, 0, `REPEATED empty value`);
    notStrictEqual(schemata[0].mode, 'REPEATED', `REPEATED REPEATED not valid`);
    schemata.forEach(s => deepStrictEqual(s, schemata[0], `REPEATED of varying shape`));
    fieldSchema = schemata[0];
    fieldSchema.mode = 'REPEATED';
    fieldValue = entries.map(c => c[1]);
  } else if (isRecord(value)) {
    fieldSchema.type = 'RECORD';
    fieldSchema.fields = [];
    const recordFieldValue: {[_: string]: unknown} = {};
    Object.entries(value).sort((a, b) => a[0].localeCompare(b[0])).forEach(([k, v]) => {
      try {
        const [entrySchema, entryValue] = nameValueToSchemaValueTuple(k, v);
        fieldSchema.fields?.push(entrySchema);
        recordFieldValue[k] = entryValue;
      } catch (e) {
        console.error(`IGNORE ${name}.${k} :: ${e.message}`);
      }
    });
    notStrictEqual(Object.entries(recordFieldValue).length, 0, 'empty RECORD');
    fieldValue = recordFieldValue;
  }

  return [fieldSchema, fieldValue];
}

function discoverSchema(value: unknown): [TableField[], unknown] {
  const [rootSchema, rootValue] = nameValueToSchemaValueTuple('root', value);
  const rootFields = rootSchema.fields;
  if (rootFields && rootValue) {
    return [rootFields, rootValue];
  } else {
    throw new Error('Invalid data ' + JSON.stringify(value));
  }
}

export async function insertToBq(datasetId: string, tablePrefix: string, data: unknown, insertId?: string) {
  ok(datasetId.match(/^[a-zA-Z0-9_]+$/), `invalid dataset "${datasetId}"`);
  ok(tablePrefix.match(/^[a-zA-Z0-9_]+$/), `invalid table prefix "${tablePrefix}"`);
  const [schema, value] = discoverSchema(data);
  const schemaId = createHash('sha256').update(JSON.stringify(schema)).digest('base64').replace(/[^a-zA-Z0-9]/g, '');
  const tableId = `${tablePrefix}_${schemaId}`;

  const bq = new BigQuery();
  const dataset = bq.dataset(datasetId);

  try {
    await dataset.table(tableId).insert(
      { insertId, json: value },
      { raw: true }
    );
    console.log(`INSERT :: ${datasetId}.${tableId} :: ${JSON.stringify(value)}`);
    return true;
  } catch (e) {
    if (e.code === 404) {
      const tableExists = (await dataset.table(tableId).exists())[0];
      if (!tableExists) {
        await dataset.createTable(tableId, { schema });
        console.log(`CREATE :: ${datasetId}.${tableId} :: ${JSON.stringify(schema)}`);
      }
      return false;
    } else {
      throw e;
    }
  }
}
