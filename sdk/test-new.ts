import { Lux } from './src/index';

const db = new Lux({ host: 'localhost', port: 6399 });

async function assert(name: string, fn: () => Promise<void>) {
    try {
        await fn();
        console.log(`  PASS: ${name}`);
    } catch (e: any) {
        console.log(`  FAIL: ${name} - ${e.message}`);
        process.exit(1);
    }
}

function sleep(ms: number) {
    return new Promise((r) => setTimeout(r, ms));
}

async function run() {
    await db.call('FLUSHALL');
    console.log('@luxdb/sdk time series + ksub tests\n');

    await assert('tsadd with auto timestamp', async () => {
        const ts = await db.tsadd('cpu:host1', '*', 72.5, {
            retention: 86400000,
            labels: { host: 'server1', metric: 'cpu' },
        });
        if (typeof ts !== 'number' && typeof ts !== 'string') throw new Error(`expected timestamp, got ${ts}`);
    });

    await assert('tsadd more samples', async () => {
        await sleep(50);
        await db.tsadd('cpu:host1', '*', 75.0);
        await sleep(50);
        await db.tsadd('cpu:host1', '*', 68.2);
    });

    await assert('tsget returns latest sample', async () => {
        const sample = await db.tsget('cpu:host1');
        if (!sample) throw new Error('expected sample');
        if (typeof sample.timestamp !== 'number') throw new Error(`expected number timestamp, got ${typeof sample.timestamp}`);
        if (Math.abs(sample.value - 68.2) > 0.01) throw new Error(`expected ~68.2, got ${sample.value}`);
    });

    await assert('tsget returns null for missing key', async () => {
        const sample = await db.tsget('nonexistent:ts');
        if (sample !== null) throw new Error('expected null');
    });

    await assert('tsrange returns samples', async () => {
        const samples = await db.tsrange('cpu:host1', '-', '+');
        if (samples.length < 3) throw new Error(`expected >= 3 samples, got ${samples.length}`);
        if (typeof samples[0].timestamp !== 'number') throw new Error('expected number timestamp');
        if (typeof samples[0].value !== 'number') throw new Error('expected number value');
    });

    await assert('tsrange with aggregation', async () => {
        const samples = await db.tsrange('cpu:host1', '-', '+', {
            aggregation: { type: 'avg', bucketSize: 999999999 },
        });
        if (samples.length < 1) throw new Error(`expected >= 1 bucket, got ${samples.length}`);
        const avg = samples[0].value;
        if (avg < 60 || avg > 80) throw new Error(`expected avg ~71.9, got ${avg}`);
    });

    await assert('tsmadd batch insert', async () => {
        await db.tsmadd(['mem:host1', '*', 45.0], ['disk:host1', '*', 82.1]);
        const mem = await db.tsget('mem:host1');
        if (!mem) throw new Error('expected mem sample');
        if (Math.abs(mem.value - 45.0) > 0.01) throw new Error(`expected 45.0, got ${mem.value}`);
    });

    await assert('tsinfo returns metadata', async () => {
        const info = await db.tsinfo('cpu:host1');
        if (!info) throw new Error('expected info');
        if (info['totalSamples'] === undefined && info['total_samples'] === undefined) {
            const keys = Object.keys(info);
            console.log(`    info keys: ${keys.join(', ')}`);
        }
    });

    await assert('ksub receives set events', async () => {
        const events: any[] = [];
        const sub = db.ksub(['test:*'], (event) => {
            events.push(event);
        });

        await sleep(200);
        await db.set('test:hello', 'world');
        await sleep(200);

        if (events.length === 0) throw new Error('expected at least 1 event');
        if (events[0].key !== 'test:hello') throw new Error(`expected key test:hello, got ${events[0].key}`);
        if (events[0].operation !== 'set') throw new Error(`expected op set, got ${events[0].operation}`);
        if (events[0].pattern !== 'test:*') throw new Error(`expected pattern test:*, got ${events[0].pattern}`);

        await sub.unsubscribe();
    });

    await assert('ksub filters non-matching keys', async () => {
        const events: any[] = [];
        const sub = db.ksub(['only:*'], (event) => {
            events.push(event);
        });

        await sleep(200);
        await db.set('other:key', 'val');
        await sleep(200);

        if (events.length !== 0) throw new Error(`expected 0 events, got ${events.length}`);
        await sub.unsubscribe();
    });

    await assert('ksub receives hset events', async () => {
        const events: any[] = [];
        const sub = db.ksub(['hash:*'], (event) => {
            events.push(event);
        });

        await sleep(200);
        await db.hset('hash:user1', 'name', 'alice');
        await sleep(200);

        if (events.length === 0) throw new Error('expected at least 1 event');
        if (events[0].operation !== 'hset') throw new Error(`expected hset, got ${events[0].operation}`);

        await sub.unsubscribe();
    });

    console.log('\nAll tests passed.');
    db.disconnect();
}

run().catch((e) => {
    console.error(e);
    db.disconnect();
    process.exit(1);
});
