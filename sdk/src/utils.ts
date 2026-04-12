import type { LuxError, LuxResult } from './types';

export function ok<T>(data: T): LuxResult<T> {
	return { data, error: null };
}

export function err<T>(code: string, message: string, details?: unknown): LuxResult<T> {
	return { data: null, error: { code, message, details } };
}

export function toLuxError(error: unknown, fallbackCode = 'LUX_SDK_ERROR'): LuxError {
	if (typeof error === 'object' && error && 'message' in error) {
		return { code: fallbackCode, message: String((error as { message: unknown }).message), details: error };
	}
	return { code: fallbackCode, message: String(error) };
}

