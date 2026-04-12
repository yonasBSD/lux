import type Redis from 'ioredis';
import type { KSubEvent } from './types';

interface RealtimeClient {
	duplicate(): Redis;
}

export class LuxRealtimeManager {
	private client: RealtimeClient;
	private connection: Redis | null = null;
	private initPromise: Promise<void> | null = null;
	private nextHandlerId = 1;
	private handlersByPattern = new Map<string, Map<number, (event: KSubEvent) => void>>();

	constructor(client: RealtimeClient) {
		this.client = client;
	}

	private async ensureConnection(): Promise<void> {
		if (this.connection) return;
		if (this.initPromise) return this.initPromise;

		this.initPromise = (async () => {
			const sub = this.client.duplicate();
			sub.on('error', () => {});

			const dispatch = (event: KSubEvent) => {
				const handlers = this.handlersByPattern.get(event.pattern);
				if (!handlers) return;
				for (const handler of handlers.values()) {
					handler(event);
				}
			};

			const dataHandler = (sub as any)._dataHandler || (sub as any).dataHandler;
			if (dataHandler && dataHandler.returnReply) {
				const origReturn = dataHandler.returnReply.bind(dataHandler);
				dataHandler.returnReply = (reply: any) => {
					if (Array.isArray(reply) && reply.length === 4 && reply[0] === 'kmessage') {
						dispatch({ pattern: reply[1], key: reply[2], operation: reply[3] });
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
							dispatch({ pattern: match[1], key: match[2], operation: match[3] });
							return true;
						}
					}
					return origEmit(event, ...args);
				};
			}

			this.connection = sub;
		})();

		try {
			await this.initPromise;
		} finally {
			this.initPromise = null;
		}
	}

	async subscribe(pattern: string, handler: (event: KSubEvent) => void): Promise<() => void> {
		await this.ensureConnection();

		const id = this.nextHandlerId++;
		let handlers = this.handlersByPattern.get(pattern);
		const firstForPattern = !handlers;
		if (!handlers) {
			handlers = new Map();
			this.handlersByPattern.set(pattern, handlers);
		}
		handlers.set(id, handler);

		if (firstForPattern && this.connection) {
			await (this.connection as any).call('KSUB', pattern);
		}

		return () => {
			const map = this.handlersByPattern.get(pattern);
			if (!map) return;
			map.delete(id);
			if (map.size === 0) {
				this.handlersByPattern.delete(pattern);
				if (this.connection) {
					(this.connection as any).call('KUNSUB', pattern).catch(() => {});
				}
			}
		};
	}
}

