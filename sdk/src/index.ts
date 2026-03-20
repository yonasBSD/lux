import Redis, { RedisOptions } from 'ioredis';

export interface VSearchResult {
    key: string;
    similarity: number;
    metadata?: Record<string, unknown>;
}

export interface VSearchOptions {
    k: number;
    filter?: { key: string; value: string };
    meta?: boolean;
}

export interface TSSample {
    timestamp: number;
    value: number;
}

export interface TSAddOptions {
    retention?: number;
    labels?: Record<string, string>;
}

export interface TSRangeOptions {
    aggregation?: {
        type: 'avg' | 'sum' | 'min' | 'max' | 'count' | 'first' | 'last' | 'range' | 'std.p' | 'std.s' | 'var.p' | 'var.s';
        bucketSize: number;
    };
}

export interface TSMRangeResult {
    key: string;
    labels: Record<string, string>;
    samples: TSSample[];
}

export interface KSubEvent {
    pattern: string;
    key: string;
    operation: string;
}

export class Lux extends Redis {
    constructor(options?: RedisOptions | string) {
        super(options as any);
    }

    async vset(
        key: string,
        vector: number[],
        options?: { metadata?: Record<string, unknown>; ex?: number; px?: number }
    ): Promise<'OK'> {
        const args: (string | number)[] = [key, vector.length, ...vector];
        if (options?.metadata) {
            args.push('META', JSON.stringify(options.metadata));
        }
        if (options?.ex) {
            args.push('EX', options.ex);
        } else if (options?.px) {
            args.push('PX', options.px);
        }
        return this.call('VSET', ...args) as Promise<'OK'>;
    }

    async vget(key: string): Promise<{ dims: number; vector: number[]; metadata?: Record<string, unknown> } | null> {
        const result = (await this.call('VGET', key)) as any[] | null;
        if (!result || !Array.isArray(result)) return null;

        const dims = parseInt(result[0], 10);
        const vector: number[] = [];
        for (let i = 1; i <= dims; i++) {
            vector.push(parseFloat(result[i]));
        }
        const metaRaw = result[dims + 1];
        let metadata: Record<string, unknown> | undefined;
        if (metaRaw) {
            try {
                metadata = JSON.parse(metaRaw);
            } catch {}
        }
        return { dims, vector, metadata };
    }

    async vsearch(query: number[], options: VSearchOptions): Promise<VSearchResult[]> {
        const args: (string | number)[] = [query.length, ...query, 'K', options.k];
        if (options.filter) {
            args.push('FILTER', options.filter.key, options.filter.value);
        }
        if (options.meta) {
            args.push('META');
        }
        const result = (await this.call('VSEARCH', ...args)) as any[] | null;
        if (!result || !Array.isArray(result)) return [];

        const results: VSearchResult[] = [];
        for (const item of result) {
            if (Array.isArray(item)) {
                const entry: VSearchResult = {
                    key: item[0],
                    similarity: parseFloat(item[1]),
                };
                if (options.meta && item[2]) {
                    try {
                        entry.metadata = JSON.parse(item[2]);
                    } catch {
                        entry.metadata = { _raw: item[2] };
                    }
                }
                results.push(entry);
            }
        }
        return results;
    }

    async vcard(): Promise<number> {
        return this.call('VCARD') as Promise<number>;
    }

    async tsadd(key: string, timestamp: number | '*', value: number, options?: TSAddOptions): Promise<number> {
        const args: (string | number)[] = [key, timestamp === '*' ? '*' : timestamp, value];
        if (options?.retention != null) {
            args.push('RETENTION', options.retention);
        }
        if (options?.labels) {
            args.push('LABELS');
            for (const [k, v] of Object.entries(options.labels)) {
                args.push(k, v);
            }
        }
        return this.call('TSADD', ...args) as Promise<number>;
    }

    async tsmadd(...entries: [string, number | '*', number][]): Promise<string> {
        const args: (string | number)[] = [];
        for (const [key, ts, val] of entries) {
            args.push(key, ts === '*' ? '*' : ts, val);
        }
        return this.call('TSMADD', ...args) as Promise<string>;
    }

    async tsget(key: string): Promise<TSSample | null> {
        const result = (await this.call('TSGET', key)) as any[] | null;
        if (!result || !Array.isArray(result) || result.length < 2) return null;
        return { timestamp: parseInt(result[0], 10), value: parseFloat(result[1]) };
    }

    async tsrange(key: string, from: number | '-', to: number | '+', options?: TSRangeOptions): Promise<TSSample[]> {
        const args: (string | number)[] = [key, from === '-' ? '-' : from, to === '+' ? '+' : to];
        if (options?.aggregation) {
            args.push('AGGREGATION', options.aggregation.type, options.aggregation.bucketSize);
        }
        const result = (await this.call('TSRANGE', ...args)) as any[] | null;
        if (!result || !Array.isArray(result)) return [];
        return result.map((pair: any[]) => ({
            timestamp: parseInt(pair[0], 10),
            value: parseFloat(pair[1]),
        }));
    }

    async tsmrange(from: number | '-', to: number | '+', filter: string, options?: TSRangeOptions): Promise<TSMRangeResult[]> {
        const args: (string | number)[] = [from === '-' ? '-' : from, to === '+' ? '+' : to];
        if (options?.aggregation) {
            args.push('AGGREGATION', options.aggregation.type, options.aggregation.bucketSize);
        }
        args.push('FILTER', filter);
        const result = (await this.call('TSMRANGE', ...args)) as any[] | null;
        if (!result || !Array.isArray(result)) return [];
        return result.map((series: any[]) => {
            const labels: Record<string, string> = {};
            if (Array.isArray(series[1])) {
                for (const pair of series[1]) {
                    if (Array.isArray(pair) && pair.length >= 2) {
                        labels[pair[0]] = pair[1];
                    }
                }
            }
            const samples: TSSample[] = Array.isArray(series[2])
                ? series[2].map((s: any[]) => ({ timestamp: parseInt(s[0], 10), value: parseFloat(s[1]) }))
                : [];
            return { key: series[0], labels, samples };
        });
    }

    async tsinfo(key: string): Promise<Record<string, unknown>> {
        const result = (await this.call('TSINFO', key)) as any[] | null;
        if (!result || !Array.isArray(result)) return {};
        const info: Record<string, unknown> = {};
        for (let i = 0; i < result.length - 1; i += 2) {
            const k = result[i];
            const v = result[i + 1];
            if (k === 'labels' && Array.isArray(v)) {
                const labels: Record<string, string> = {};
                for (const pair of v) {
                    if (Array.isArray(pair) && pair.length >= 2) labels[pair[0]] = pair[1];
                }
                info[k] = labels;
            } else {
                info[k] = v;
            }
        }
        return info;
    }

    ksub(patterns: string[], handler: (event: KSubEvent) => void): { unsubscribe: () => void; connection: Redis } {
        const sub = this.duplicate();

        sub.on('error', () => {});

        const origReturnReply = (sub as any).returnReply?.bind(sub);
        const dataHandler = (sub as any)._dataHandler || (sub as any).dataHandler;

        if (dataHandler && dataHandler.returnReply) {
            const origReturn = dataHandler.returnReply.bind(dataHandler);
            dataHandler.returnReply = (reply: any) => {
                if (Array.isArray(reply) && reply.length === 4 && reply[0] === 'kmessage') {
                    handler({ pattern: reply[1], key: reply[2], operation: reply[3] });
                    return;
                }
                return origReturn(reply);
            };
        } else {
            const origEmit = sub.emit.bind(sub);
            (sub as any).emit = (event: string, ...args: any[]) => {
                if (event === 'error' && args[0]?.message?.includes('Command queue state error')) {
                    const match = args[0].message.match(/Last reply: kmessage,([^,]+),([^,]+),(.+)/);
                    if (match) {
                        handler({ pattern: match[1], key: match[2], operation: match[3] });
                        return true;
                    }
                }
                return origEmit(event, ...args);
            };
        }

        sub.call('KSUB', ...patterns);

        return {
            connection: sub,
            unsubscribe() {
                sub.disconnect();
            },
        };
    }
}

export default Lux;
