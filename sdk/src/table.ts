import type {
	KSubEvent,
	LuxError,
	LuxResult,
	TableChangeEvent,
	TableChangeType,
	TableErrorEvent,
	TableRow,
	TableSchema,
	VSearchResult,
} from './types';
import { err, ok, toLuxError } from './utils';

type TableWhereOp = '=' | '!=' | '>' | '<' | '>=' | '<=';
type TableWhereValue = string | number | boolean;

interface TableWhereCondition {
	field: string;
	op: TableWhereOp;
	value: TableWhereValue;
}

interface TableJoinClause {
	table: string;
	alias: string;
	onLeft: string;
	onRight: string;
}

interface TableSimilarityClause {
	field: string;
	vector: number[];
	k: number;
	filter?: { key: string; value: string };
}

interface TableClient {
	call(command: string, ...args: Array<string | number>): Promise<unknown>;
	_tselect(args: string[]): Promise<TableRow[]>;
	_subscribePattern(pattern: string, handler: (event: KSubEvent) => void): Promise<() => void>;
	vsearch(
		query: number[],
		options: { k: number; filter?: { key: string; value: string }; meta?: boolean },
	): Promise<VSearchResult[]>;
}

export interface TableQueryBuilderOptions<T extends TableRow> {
	schema?: TableSchema<T>;
}

export class TableSubscription<T extends TableRow> {
	private client: TableClient;
	private table: string;
	private selectArgsBuilder: (extra?: TableWhereCondition[]) => string[];
	private handlers: {
		change: Array<(event: TableChangeEvent<T>) => void>;
		insert: Array<(event: TableChangeEvent<T>) => void>;
		update: Array<(event: TableChangeEvent<T>) => void>;
		delete: Array<(event: TableChangeEvent<T>) => void>;
		error: Array<(event: TableErrorEvent) => void>;
	} = {
		change: [],
		insert: [],
		update: [],
		delete: [],
		error: [],
	};
	private knownRows = new Map<string, T>();
	private unsubscribeFn: (() => void) | null = null;
	private initError: LuxError | null;

	constructor(
		client: TableClient,
		table: string,
		selectArgsBuilder: (extra?: TableWhereCondition[]) => string[],
		initError: LuxError | null = null,
	) {
		this.client = client;
		this.table = table;
		this.selectArgsBuilder = selectArgsBuilder;
		this.initError = initError;
		void this.start();
	}

	on(event: 'insert' | 'update' | 'delete' | 'change', handler: (event: TableChangeEvent<T>) => void): this;
	on(event: 'error', handler: (event: TableErrorEvent) => void): this;
	on(
		event: TableChangeType,
		handler: ((event: TableChangeEvent<T>) => void) | ((event: TableErrorEvent) => void),
	): this {
		(this.handlers[event] as Array<typeof handler>).push(handler);
		return this;
	}

	async unsubscribe(): Promise<void> {
		if (this.unsubscribeFn) {
			this.unsubscribeFn();
			this.unsubscribeFn = null;
		}
	}

	private emitError(error: LuxError): void {
		for (const handler of this.handlers.error) {
			handler({ type: 'error', table: this.table, error });
		}
	}

	private emitChange(event: TableChangeEvent<T>): void {
		for (const handler of this.handlers.change) handler(event);
		for (const handler of this.handlers[event.type]) handler(event);
	}

	private extractPkFromKey(key: string): string | null {
		const prefix = `_t:${this.table}:row:`;
		if (!key.startsWith(prefix)) return null;
		return key.slice(prefix.length);
	}

	private async fetchMatches(extra?: TableWhereCondition[]): Promise<T[]> {
		const args = this.selectArgsBuilder(extra);
		const rows = await this.client._tselect(args);
		return rows as T[];
	}

	private async start(): Promise<void> {
		if (this.initError) {
			this.emitError(this.initError);
			return;
		}

		try {
			const initial = await this.fetchMatches();
			for (const row of initial) {
				if (row.id == null) continue;
				this.knownRows.set(String(row.id), row);
			}

			const pattern = `_t:${this.table}:row:*`;
			this.unsubscribeFn = await this.client._subscribePattern(pattern, (raw) => {
				void this.handleRawChange(raw);
			});
		} catch (error) {
			this.emitError(toLuxError(error, 'LUX_SUBSCRIBE_INIT_ERROR'));
		}
	}

	private async handleRawChange(raw: KSubEvent): Promise<void> {
		const pk = this.extractPkFromKey(raw.key);
		if (!pk) return;

		try {
			const previous = this.knownRows.get(pk) ?? null;
			const rows = await this.fetchMatches([{ field: 'id', op: '=', value: pk }]);
			const next = rows[0] ?? null;

			if (!previous && !next) return;

			if (!previous && next) {
				this.knownRows.set(pk, next);
				this.emitChange({
					type: 'insert',
					table: this.table,
					pk,
					operation: raw.operation,
					new: next,
					old: null,
					raw,
				});
				return;
			}

			if (previous && !next) {
				this.knownRows.delete(pk);
				this.emitChange({
					type: 'delete',
					table: this.table,
					pk,
					operation: raw.operation,
					new: null,
					old: previous,
					raw,
				});
				return;
			}

			if (!previous || !next) return;

			this.knownRows.set(pk, next);
			const changed = Object.keys(next).filter((key) => previous[key] !== next[key]);
			this.emitChange({
				type: 'update',
				table: this.table,
				pk,
				operation: raw.operation,
				new: next,
				old: previous,
				changed,
				raw,
			});
		} catch (error) {
			this.emitError(toLuxError(error, 'LUX_SUBSCRIBE_EVENT_ERROR'));
		}
	}
}

export class TableQueryBuilder<T extends TableRow = TableRow> {
	private client: TableClient;
	private name: string;
	private conditions: TableWhereCondition[] = [];
	private orderField?: string;
	private orderDir?: 'ASC' | 'DESC';
	private limitCount?: number;
	private offsetCount?: number;
	private joinClause?: TableJoinClause;
	private similarityClause?: TableSimilarityClause;
	private selectClause = '*';
	private expectSingle = false;
	private schema?: TableSchema<T>;

	constructor(client: TableClient, name: string, options?: TableQueryBuilderOptions<T>) {
		this.client = client;
		this.name = name;
		this.schema = options?.schema;
	}

	private validateRow(row: TableRow): T {
		if (!this.schema) return row as T;
		if (this.schema.safeParse) {
			const parsed = this.schema.safeParse(row);
			if (!parsed.success) {
				throw new Error('row failed schema validation');
			}
			return parsed.data;
		}
		if (this.schema.parse) {
			return this.schema.parse(row);
		}
		return row as T;
	}

	private buildSelectArgs(extra?: TableWhereCondition[]): string[] {
		const args: string[] = [this.selectClause, 'FROM', this.name];
		const allConditions = extra ? [...this.conditions, ...extra] : this.conditions;

		if (this.joinClause) {
			args.push(
				'JOIN',
				this.joinClause.table,
				this.joinClause.alias,
				'ON',
				this.joinClause.onLeft,
				'=',
				this.joinClause.onRight,
			);
		}

		if (allConditions.length) {
			args.push('WHERE');
			for (let i = 0; i < allConditions.length; i++) {
				const cond = allConditions[i];
				args.push(cond.field, cond.op, String(cond.value));
				if (i < allConditions.length - 1) {
					args.push('AND');
				}
			}
		}

		if (this.orderField) {
			args.push('ORDER', 'BY', this.orderField, this.orderDir || 'ASC');
		}
		if (this.limitCount != null) {
			args.push('LIMIT', String(this.limitCount));
		}
		if (this.offsetCount != null) {
			args.push('OFFSET', String(this.offsetCount));
		}

		return args;
	}

	select(columns = '*'): this {
		this.selectClause = columns;
		return this;
	}

	single(): this {
		this.expectSingle = true;
		if (this.limitCount == null) {
			this.limitCount = 1;
		}
		return this;
	}

	where(field: string, op: TableWhereOp, value: TableWhereValue): this {
		this.conditions.push({ field, op, value });
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

	offset(n: number): this {
		this.offsetCount = n;
		return this;
	}

	join(table: string, alias: string, onLeft: string, onRight: string): this {
		this.joinClause = { table, alias, onLeft, onRight };
		return this;
	}

	similar(field: string, vector: number[], options: { k: number; filter?: { key: string; value: string } }): this {
		this.similarityClause = {
			field,
			vector,
			k: options.k,
			filter: options.filter,
		};
		return this;
	}

	private parseSimilarityPk(result: VSearchResult, field: string): string | null {
		const metadata = result.metadata;
		if (metadata && typeof metadata === 'object') {
			for (const key of ['id', 'pk', 'row_id']) {
				const value = (metadata as Record<string, unknown>)[key];
				if (value != null) return String(value);
			}
		}

		const expectedPrefix = `${this.name}:${field}:`;
		if (result.key.startsWith(expectedPrefix)) {
			return result.key.slice(expectedPrefix.length);
		}

		const segments = result.key.split(':');
		if (segments.length > 0) {
			return segments[segments.length - 1] || null;
		}

		return null;
	}

	async run(): Promise<LuxResult<T[] | T>> {
		try {
			let rows: TableRow[] = [];

			if (this.similarityClause) {
				if (this.joinClause) {
					return err(
						'SIMILAR_JOIN_UNSUPPORTED',
						'similar(...) cannot be combined with join(...) yet',
					);
				}

				const similarResults = await this.client.vsearch(this.similarityClause.vector, {
					k: this.similarityClause.k,
					filter: this.similarityClause.filter,
					meta: true,
				});

				for (const match of similarResults) {
					const pk = this.parseSimilarityPk(match, this.similarityClause.field);
					if (!pk) continue;

					const args = this.buildSelectArgs([{ field: 'id', op: '=', value: pk }]);
					const one = await this.client._tselect(args);
					if (one.length === 0) continue;

					rows.push({ ...one[0], _similarity: match.similarity });
				}

				if (this.offsetCount != null || this.limitCount != null) {
					const start = this.offsetCount ?? 0;
					const end = this.limitCount != null ? start + this.limitCount : undefined;
					rows = rows.slice(start, end);
				}
			} else {
				rows = await this.client._tselect(this.buildSelectArgs());
			}

			const validated = rows.map((row) => this.validateRow(row));

			if (this.expectSingle) {
				if (validated.length === 0) {
					return err('NOT_FOUND', `No rows found in table '${this.name}'`);
				}
				return ok(validated[0]);
			}

			return ok(validated);
		} catch (error) {
			return err('TSELECT_ERROR', `Failed to query table '${this.name}'`, toLuxError(error));
		}
	}

	async insert(data: Record<string, unknown>): Promise<LuxResult<number>> {
		try {
			if (this.schema) {
				this.validateRow(data as TableRow);
			}
			const args: (string | number)[] = [this.name];
			for (const [k, v] of Object.entries(data)) {
				args.push(k, String(v));
			}
			const result = await this.client.call('TINSERT', ...args) as string;
			return ok(parseInt(result, 10) || 0);
		} catch (error) {
			return err('TINSERT_ERROR', `Failed to insert into '${this.name}'`, toLuxError(error));
		}
	}

	async update(id: number | string, data: Record<string, unknown>): Promise<LuxResult<number>> {
		try {
			const args: (string | number)[] = [this.name, 'SET'];
			for (const [k, v] of Object.entries(data)) {
				args.push(k, String(v));
			}
			args.push('WHERE', 'id', '=', String(id));
			const result = await this.client.call('TUPDATE', ...args) as string | number;
			return ok(Number(result) || 0);
		} catch (error) {
			return err('TUPDATE_ERROR', `Failed to update '${this.name}'`, toLuxError(error));
		}
	}

	async updateWhere(data: Record<string, unknown>): Promise<LuxResult<number>> {
		if (this.conditions.length === 0) {
			return err('MISSING_WHERE', 'updateWhere requires at least one where() condition');
		}
		try {
			const args: (string | number)[] = [this.name, 'SET'];
			for (const [k, v] of Object.entries(data)) {
				args.push(k, String(v));
			}
			args.push('WHERE');
			for (let i = 0; i < this.conditions.length; i++) {
				const cond = this.conditions[i];
				args.push(cond.field, cond.op, String(cond.value));
				if (i < this.conditions.length - 1) {
					args.push('AND');
				}
			}
			const result = await this.client.call('TUPDATE', ...args) as string | number;
			return ok(Number(result) || 0);
		} catch (error) {
			return err('TUPDATE_ERROR', `Failed to update '${this.name}'`, toLuxError(error));
		}
	}

	async delete(...ids: Array<number | string>): Promise<LuxResult<number>> {
		try {
			let deleted = 0;
			for (const id of ids) {
				const result = await this.client.call('TDELETE', 'FROM', this.name, 'WHERE', 'id', '=', String(id)) as string | number;
				deleted += Number(result) || 0;
			}
			return ok(deleted);
		} catch (error) {
			return err('TDELETE_ERROR', `Failed to delete from '${this.name}'`, toLuxError(error));
		}
	}

	async deleteWhere(): Promise<LuxResult<number>> {
		if (this.conditions.length === 0) {
			return err('MISSING_WHERE', 'deleteWhere requires at least one where() condition');
		}
		try {
			const args: (string | number)[] = ['FROM', this.name, 'WHERE'];
			for (let i = 0; i < this.conditions.length; i++) {
				const cond = this.conditions[i];
				args.push(cond.field, cond.op, String(cond.value));
				if (i < this.conditions.length - 1) {
					args.push('AND');
				}
			}
			const result = await this.client.call('TDELETE', ...args) as string | number;
			return ok(Number(result) || 0);
		} catch (error) {
			return err('TDELETE_ERROR', `Failed to delete from '${this.name}'`, toLuxError(error));
		}
	}

	subscribe(): TableSubscription<T> {
		if (this.similarityClause) {
			return new TableSubscription<T>(
				this.client,
				this.name,
				(extra) => this.buildSelectArgs(extra),
				{
					code: 'SIMILAR_SUBSCRIBE_UNSUPPORTED',
					message: 'subscribe() is not supported on similar(...) queries yet',
				},
			);
		}
		return new TableSubscription<T>(this.client, this.name, (extra) => this.buildSelectArgs(extra));
	}
}

