// Install shipdit@0.6.0 in a temporary directory, then run with
// SHIPDIT_MODULE=/absolute/path/to/node_modules/shipdit/dist/index.mjs node scripts/generate-parity.mjs
import { readFileSync, writeFileSync } from 'node:fs';
import { gzipSync } from 'node:zlib';
const { createClient, SDK_PACKAGE_VERSION } = await import(process.env.SHIPDIT_MODULE || 'shipdit');
if (SDK_PACKAGE_VERSION !== '0.6.0') throw new Error(`Expected shipdit 0.6.0, got ${SDK_PACKAGE_VERSION}`);
const { snapshot, cases: shared } = JSON.parse(readFileSync(new URL('../testdata/evaluation.json', import.meta.url)));
const client = createClient({ sdkKey: 'parity-test', events: false, refreshIntervalMs: 0, fetch: async () => Response.json(snapshot) });
await client.ready();
if (client.getVersion() !== snapshot.version) {
 await client.close();
 throw new Error(`Snapshot failed to load: expected version ${snapshot.version}, got ${client.getVersion()}`);
}
const methods = {boolean:'isEnabled',variant:'getVariant',number:'getNumber',json:'getJson'};
const cases = [];
const contexts = shared.map(c => c.context);
for (let i=0;i<256;i++) contexts.push({userId:`${['user','用户','😀','𝒜'][i%4]}-${i}`,attributes:{plan:['free','pro','enterprise'][i%3],age:i,teamId:`team-${i%13}`,active:i%2===0},segmentIds:i%5===0?['seg-vip']:[]});
for (const context of contexts) for (const [key, flag] of Object.entries(snapshot.flags)) {
 const method=methods[flag.type]; cases.push({key,method,context,expected:client[method](key,context)});
}
for (const method of Object.values(methods)) cases.push({key:'missing',method,context:{},expected:client[method]('missing',{})});
await client.close();
writeFileSync(new URL('../testdata/published-sdk.json.gz',import.meta.url),gzipSync(JSON.stringify({package:`shipdit@${SDK_PACKAGE_VERSION}`,snapshot,cases})+'\n'));
console.log(`Generated ${cases.length} evaluations from shipdit@${SDK_PACKAGE_VERSION}`);
