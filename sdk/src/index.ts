import Redis, { type RedisOptions } from 'ioredis';

export interface VSearchResult {
	key: string;
	similarity: number;
	metadata?: Record<string, unknown>;
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

export interface TableRow {
	id: number;
	[field: string]: unknown;
}

class TableQueryBuilder {
	private client: Lux;
	private name: string;
	private conditions: string[] = [];
	private orderField?: string;
	private orderDir?: 'ASC' | 'DESC';
	private limitCount?: number;
	private joinTable?: string;

	constructor(client: Lux, name: string) {
		this.client = client;
		this.name = name;
	}

	where(field: string, op: '=' | '!=' | '>' | '<' | '>=' | '<=', value: string | number | boolean): this {
		const v = typeof value === 'string' ? `'${value}'` : String(value);
		this.conditions.push(`${field} ${op} ${v}`);
		return this;
	}

	orderBy(field: string, dir: 'asc' | 'desc' = 'asc'): this {
		this.orderField = field;
		this.orderDir = dir.toUpperCase() as 'ASC' | 'DESC';
		return this;
	}

	limit(n: number): this {
		this.limitCount = n;
		return this;
	}

	join(table: string): this {
		this.joinTable = table;
		return this;
	}

	async run(): Promise<TableRow[]> {
		const args: string[] = [this.name];
		if (this.conditions.length) {
			args.push('WHERE', this.conditions.join(' AND '));
		}
		if (this.orderField) {
			args.push('ORDER', 'BY', this.orderField, this.orderDir || 'ASC');
		}
		if (this.limitCount != null) {
			args.push('LIMIT', String(this.limitCount));
		}
		if (this.joinTable) {
			args.push('JOIN', this.joinTable);
		}
		return this.client._tquery(args);
	}

	async insert(data: Record<string, unknown>): Promise<number> {
		const args: (string | number)[] = [this.name];
		for (const [k, v] of Object.entries(data)) {
			args.push(k, String(v));
		}
		const result = await this.client.call('TINSERT', ...args) as string;
		return parseInt(result, 10) || 0;
	}

	async update(id: number, data: Record<string, unknown>): Promise<string> {
		const args: (string | number)[] = [this.name, id];
		for (const [k, v] of Object.entries(data)) {
			args.push(k, String(v));
		}
		return this.client.call('TUPDATE', ...args) as Promise<string>;
	}

	async delete(...ids: number[]): Promise<number> {
		return this.client.call('TDEL', this.name, ...ids) as Promise<number>;
	}
}

class VectorNamespace {
	private client: Lux;

	constructor(client: Lux) {
		this.client = client;
	}

	async set(key: string, vector: number[], metadata?: Record<string, unknown>): Promise<string> {
		const args: (string | number)[] = [key, vector.length, ...vector];
		if (metadata) {
			args.push('META', JSON.stringify(metadata));
		}
		return this.client.call('VSET', ...args) as Promise<string>;
	}

	async get(key: string): Promise<{ dims: number; vector: number[]; metadata?: Record<string, unknown> } | null> {
		return this.client.vget(key);
	}

	async search(query: number[], options: { topK: number; filter?: { key: string; value: string }; meta?: boolean }): Promise<VSearchResult[]> {
		return this.client.vsearch(query, { k: options.topK, filter: options.filter, meta: options.meta ?? true });
	}

	async count(): Promise<number> {
		return this.client.vcard();
	}
}

class TimeSeriesNamespace {
	private client: Lux;

	constructor(client: Lux) {
		this.client = client;
	}

	async add(key: string, value: number, options?: { timestamp?: number | '*'; retention?: number; labels?: Record<string, string> }): Promise<number> {
		return this.client.tsadd(key, options?.timestamp ?? '*', value, { retention: options?.retention, labels: options?.labels });
	}

	async get(key: string): Promise<TSSample | null> {
		return this.client.tsget(key);
	}

	async range(key: string, from: number | '-', to: number | '+', options?: TSRangeOptions): Promise<TSSample[]> {
		return this.client.tsrange(key, from, to, options);
	}

	async mrange(from: number | '-', to: number | '+', filter: string, options?: TSRangeOptions): Promise<TSMRangeResult[]> {
		return this.client.tsmrange(from, to, filter, options);
	}

	async info(key: string): Promise<Record<string, unknown>> {
		return this.client.tsinfo(key);
	}
}

export class Lux extends Redis {
	vectors: VectorNamespace;
	timeseries: TimeSeriesNamespace;

	constructor(options?: RedisOptions | string) {
		if (typeof options === 'string') {
			if (options.startsWith('rediss://') || options.startsWith('luxs://')) {
				throw new Error('TLS is not yet supported');
			}
			options = options.replace(/^lux:\/\//, 'redis://');
		}
		super(options as any);
		this.vectors = new VectorNamespace(this);
		this.timeseries = new TimeSeriesNamespace(this);
	}

	table(name: string): TableQueryBuilder {
		return new TableQueryBuilder(this, name);
	}

	async _tquery(args: string[]): Promise<TableRow[]> {
		const result = await this.call('TQUERY', ...args) as any;
		if (!result || !Array.isArray(result)) return [];

		const rows: TableRow[] = [];
		for (const item of result) {
			if (Array.isArray(item) && item.length >= 2) {
				const row: TableRow = { id: 0 };
				for (let i = 0; i < item.length - 1; i += 2) {
					const key = String(item[i]);
					const val = item[i + 1];
					if (key === 'id') {
						row.id = parseInt(val, 10);
					} else {
						row[key] = val;
					}
				}
				rows.push(row);
			}
		}
		return rows;
	}

	// Vector methods (keep for backward compat)
	async vset(key: string, vector: number[], options?: { metadata?: Record<string, unknown>; ex?: number; px?: number }): Promise<'OK'> {
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
		const result = await this.call('VGET', key) as any;
		if (!result || !Array.isArray(result)) return null;
		const dims = parseInt(result[0], 10);
		const vector: number[] = [];
		for (let i = 1; i <= dims; i++) {
			vector.push(parseFloat(result[i]));
		}
		const metaRaw = result[dims + 1];
		let metadata: Record<string, unknown> | undefined;
		if (metaRaw) {
			try { metadata = JSON.parse(metaRaw); } catch {}
		}
		return { dims, vector, metadata };
	}

	async vsearch(query: number[], options: { k: number; filter?: { key: string; value: string }; meta?: boolean }): Promise<VSearchResult[]> {
		const args: (string | number)[] = [query.length, ...query, 'K', options.k];
		if (options.filter) {
			args.push('FILTER', options.filter.key, options.filter.value);
		}
		if (options.meta) {
			args.push('META');
		}
		const result = await this.call('VSEARCH', ...args) as any;
		if (!result || !Array.isArray(result)) return [];
		const results: VSearchResult[] = [];
		for (const item of result) {
			if (Array.isArray(item)) {
				const entry: VSearchResult = { key: item[0], similarity: parseFloat(item[1]) };
				if (options.meta && item[2]) {
					try { entry.metadata = JSON.parse(item[2]); } catch { entry.metadata = { _raw: item[2] }; }
				}
				results.push(entry);
			}
		}
		return results;
	}

	async vcard(): Promise<number> {
		return this.call('VCARD') as Promise<number>;
	}

	// Time series methods (keep for backward compat)
	async tsadd(key: string, timestamp: number | '*', value: number, options?: TSAddOptions): Promise<number> {
		const args: (string | number)[] = [key, timestamp === '*' ? '*' : timestamp, value];
		if (options?.retention != null) {
			args.push('RETENTION', options.retention);
		}
		if (options?.labels) {
			args.push('LABELS' as any);
			for (const [k, v] of Object.entries(options.labels)) {
				args.push(k, v as any);
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
		const result = await this.call('TSGET', key) as any;
		if (!result || !Array.isArray(result) || result.length < 2) return null;
		return { timestamp: parseInt(result[0], 10), value: parseFloat(result[1]) };
	}

	async tsrange(key: string, from: number | '-', to: number | '+', options?: TSRangeOptions): Promise<TSSample[]> {
		const args: (string | number)[] = [key, from === '-' ? '-' : from, to === '+' ? '+' : to];
		if (options?.aggregation) {
			args.push('AGGREGATION' as any, options.aggregation.type as any, options.aggregation.bucketSize);
		}
		const result = await this.call('TSRANGE', ...args) as any;
		if (!result || !Array.isArray(result)) return [];
		return result.map((pair: any) => ({ timestamp: parseInt(pair[0], 10), value: parseFloat(pair[1]) }));
	}

	async tsmrange(from: number | '-', to: number | '+', filter: string, options?: TSRangeOptions): Promise<TSMRangeResult[]> {
		const args: (string | number)[] = [from === '-' ? '-' : from, to === '+' ? '+' : to];
		if (options?.aggregation) {
			args.push('AGGREGATION' as any, options.aggregation.type as any, options.aggregation.bucketSize);
		}
		args.push('FILTER' as any, filter as any);
		const result = await this.call('TSMRANGE', ...args) as any;
		if (!result || !Array.isArray(result)) return [];
		return result.map((series: any) => {
			const labels: Record<string, string> = {};
			if (Array.isArray(series[1])) {
				for (const pair of series[1]) {
					if (Array.isArray(pair) && pair.length >= 2) labels[pair[0]] = pair[1];
				}
			}
			const samples = Array.isArray(series[2])
				? series[2].map((s: any) => ({ timestamp: parseInt(s[0], 10), value: parseFloat(s[1]) }))
				: [];
			return { key: series[0], labels, samples };
		});
	}

	async tsinfo(key: string): Promise<Record<string, unknown>> {
		const result = await this.call('TSINFO', key) as any;
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

	// Realtime key subscriptions
	ksub(patterns: string[], handler: (event: KSubEvent) => void): { unsubscribe: () => void; connection: Redis } {
		const sub = this.duplicate();
		sub.on('error', () => {});

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
			unsubscribe() { sub.disconnect(); },
		};
	}
}

export default Lux;
