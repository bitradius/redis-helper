// Copyright (c) 2019-2021, BitRadius Holdings, LLC
//
// Please see the included LICENSE file for more information.

import { RedisClient, createClient } from 'redis';
import { EventEmitter } from 'events';

export default class extends EventEmitter {
    private readonly m_client: RedisClient;
    public ttl = 60;

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

    public get client (): RedisClient {
        return this.m_client;
    }

    public async end (flush = true): Promise<void> {
        this.m_client.end(flush);
    }

    public async get<T> (key: any): Promise<T> {
        return new Promise((resolve, reject) => {
            this.m_client.get(
                typeof key !== 'string' ? JSON.stringify(key) : key,
                (error, reply) => {
                    if (error) {
                        return reject(error);
                    }

                    if (reply) {
                        const parsed = JSON.parse(reply);

                        this.emit('get', key, parsed);

                        return resolve(parsed);
                    }

                    this.emit('get', key, {});

                    return resolve({} as T);
                });
        });
    }

    public async quit (): Promise<void> {
        return new Promise(resolve => {
            this.m_client.quit(() => {
                return resolve();
            });
        });
    }

    public async set<T> (key: any, value: T, ttl = this.ttl): Promise<void> {
        return new Promise((resolve, reject) => {
            this.m_client.set(
                typeof key !== 'string' ? JSON.stringify(key) : key,
                JSON.stringify(value),
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

    public async unref (): Promise<void> {
        this.m_client.unref();
    }
}
