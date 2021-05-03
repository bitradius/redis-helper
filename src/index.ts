// Copyright (c) 2019-2021, BitRadius Holdings, LLC
//
// Please see the included LICENSE file for more information.

import { RedisClient, createClient } from 'redis';
import { EventEmitter } from 'events';

export default class extends EventEmitter {
    private readonly m_client: RedisClient;
    private m_ttl = 60;

    /**
     * Constructs a new Redis helper instance
     * @param port
     * @param host
     * @param password
     * @param database
     */
    constructor (
        private port = 6379,
        private host = '127.0.0.1',
        private password?: string,
        private database?: string
    ) {
        super();

        this.m_client = createClient(port, host, {
            password: password,
            db: database
        });

        this.m_client.on('ready', () => this.emit('ready'));
        this.m_client.on('connect', () => this.emit('connect', port, host));
        this.m_client.on('reconnecting', () => this.emit('reconnecting', port, host));
        this.m_client.on('error', (error: Error) => this.emit('error', error));
        this.m_client.on('end', () => this.emit('end'));
        this.m_client.on('warning', (warning: Error) => this.emit('warning', warning));
    }

    public on(event: 'ready', listener: () => void): this;

    public on(event: 'connect', listener: (port: number, host: string) => void): this;

    public on(event: 'reconnecting', listener: (port: number, host: string) => void): this;

    public on(event: 'error', listener: (error: Error) => void): this;

    public on(event: 'end', listener: () => void): this;

    public on(event: 'get', listener: (key: any, value: any) => void): this;

    public on(event: 'set', listener: (key: any, value: any, ttl: number) => void): this;

    public on (event: any, listener: (...args: any[]) => void): this {
        return super.on(event, listener);
    }

    /**
     * Returns the underlying RedisClient instance
     */
    public get client (): RedisClient {
        return this.m_client;
    }

    /**
     * Deletes the value at the specified key from the underlying RedisClient instance
     * @param key
     * @param nothrow
     */
    public async del (key: any, nothrow = false): Promise<void> {
        return new Promise((resolve, reject) => {
            this.m_client.del(this.stringify(key), error => {
                if (error && !nothrow) {
                    return reject(error);
                }

                return resolve();
            });
        });
    }

    /**
     * Ends the underlying RedisClient instance
     * @param flush
     */
    public async end (flush = true): Promise<void> {
        this.m_client.end(flush);
    }

    /**
     * Checks if the given key exists in the underlying RedisClient instance
     * @param key
     */
    public async exists (key: any): Promise<boolean> {
        try {
            await this.get(key);

            return true;
        } catch {
            return false;
        }
    }

    /**
     * Flushes all records from the underlying RedisClient instance
     */
    public async flush (): Promise<void> {
        return new Promise((resolve, reject) => {
            if (this.database) {
                this.m_client.flushdb(error => {
                    if (error) {
                        return reject(error);
                    }

                    return resolve();
                });
            } else {
                this.m_client.flushall(error => {
                    if (error) {
                        return reject(error);
                    }

                    return resolve();
                });
            }
        });
    }

    /**
     * Retrieves the value for the provided key
     * @param key
     */
    public async get<T> (key: any): Promise<T> {
        return new Promise((resolve, reject) => {
            this.m_client.get(
                this.stringify(key),
                (error, reply) => {
                    if (error || !reply) {
                        return reject(error);
                    }

                    const parsed = this.unstringify<T>(reply);

                    this.emit('get', key, parsed);

                    return resolve(parsed);
                });
        });
    }

    /**
     * Returns a list of all keys in the cache
     * @param pattern
     */
    public async keys (pattern = '*'): Promise<any[]> {
        return new Promise((resolve, reject) => {
            this.m_client.keys(pattern, (error, keys) => {
                if (error) {
                    return reject(error);
                }

                keys = keys.map(key => this.unstringify(key));

                return resolve(keys);
            });
        });
    }

    /**
     * Returns a map of all keys and values in the underlying RedisClient
     */
    public async list<T> (): Promise<Map<any, T>> {
        const keys = await this.keys();

        return this.mget(keys);
    }

    public async _mget (keys: string[], nothrow = false): Promise<Map<string, string>> {
        return new Promise((resolve, reject) => {
            this.m_client.mget(keys, (error, values) => {
                if (error && !nothrow) {
                    return reject(error);
                }

                const response = new Map<string, string>();

                for (let i = 0; i < keys.length; ++i) {
                    response.set(keys[i], values[i]);
                }

                return resolve(response);
            });
        });
    }

    /**
     * Mass deletes values with the specified keys from the cache
     * @param keys
     */
    public async mdel (keys: any[]): Promise<void> {
        const p = [];

        for (const key of keys) {
            p.push(this.del(key, true));
        }

        await Promise.all(p);
    }

    /**
     * Mass retrieves values (as a map) with the specified keys from the underlying RedisClient
     * @param keys
     */
    public async mget<T> (keys: any[]): Promise<Map<any, T>> {
        const fetch_keys = keys.map(key => this.stringify(key));

        const data = await this._mget(fetch_keys);

        const values: Map<any, T> = new Map<any, T>();

        for (const [key, value] of data) {
            values.set(this.unstringify(key), this.unstringify(value) as T);
        }

        return values;
    }

    /**
     * Quits the underlying RedisClient instance
     */
    public async quit (): Promise<void> {
        return new Promise(resolve => {
            this.m_client.quit(() => {
                return resolve();
            });
        });
    }

    /**
     * Sets the value for the provided key with the specified TTL (or the default)
     * @param key
     * @param value
     * @param ttl
     */
    public async set<T> (key: any, value: T, ttl = this.m_ttl): Promise<void> {
        return new Promise((resolve, reject) => {
            this.m_client.set(
                this.stringify(key),
                this.stringify(value),
                'EX',
                ttl,
                (error, reply) => {
                    if (!error && reply) {
                        this.emit('set', key, value, ttl);

                        return resolve();
                    }

                    return reject(error || new Error('Set operation failed'));
                });
        });
    }

    private stringify (str: any): string {
        return JSON.stringify(str);
    }

    /**
     * Returns the current TTL for the record at the specified key in the underlying RedisClient
     * @param key
     */
    public async ttl (key: any): Promise<number> {
        return new Promise((resolve, reject) => {
            this.m_client.ttl(this.stringify(key), (error, value) => {
                if (error) {
                    return reject(error);
                }

                return resolve(value);
            });
        });
    }

    /**
     * Unref the underlying RedisClient socket so that the thread it is attached to can be closed
     */
    public async unref (): Promise<void> {
        this.m_client.unref();
    }

    private unstringify<T> (str: any): T {
        return JSON.parse(str);
    }
}
