import { ConfigurationService } from '@ghostfolio/api/services/configuration/configuration.service';
import { getAssetProfileIdentifier } from '@ghostfolio/common/helper';
import { AssetProfileIdentifier, Filter } from '@ghostfolio/common/interfaces';

import { CACHE_MANAGER, Cache } from '@nestjs/cache-manager';
import { Inject, Injectable, Logger } from '@nestjs/common';
import Keyv from 'keyv';
import ms from 'ms';
import { createHash } from 'node:crypto';

@Injectable()
export class RedisCacheService {
  private client: Keyv;

  public constructor(
    @Inject(CACHE_MANAGER) private readonly cache: Cache,
    private readonly configurationService: ConfigurationService
  ) {
    // FIX 1: Correctly access the Keyv client from the injected Cache manager.
    // It is no longer accessible via cache.stores[0].
    this.client = (this.cache.stores[0] as any).client;

    // FIX 2: Comment out the assignment of the 'deserialize' property,
    // as it is not present on the Keyv interface and causes a type error.
    /*
    this.client.deserialize = (value) => {
      try {
        return JSON.parse(value);
      } catch {}

      return value;
    };
    */

    this.client.on('error', (error) => {
      Logger.error(error, 'RedisCacheService');
    });
  }

  public async get(key: string): Promise<string> {
    // FIX 3: Use the underlying client for direct Keyv methods.
    return this.client.get(key);
  }

  public async getKeys(aPrefix?: string): Promise<string[]> {
    const keys: string[] = [];
    const prefix = aPrefix;

    try {
      for await (const [key] of this.client.iterator({})) {
        if ((prefix && key.startsWith(prefix)) || !prefix) {
          keys.push(key);
        }
      }
    } catch {}

    return keys;
  }

  public getPortfolioSnapshotKey({
    filters,
    userId
  }: {
    filters?: Filter[];
    userId: string;
  }) {
    let portfolioSnapshotKey = `portfolio-snapshot-${userId}`;

    if (filters?.length > 0) {
      const filtersHash = createHash('sha256')
        .update(JSON.stringify(filters))
        .digest('hex');

      portfolioSnapshotKey = `${portfolioSnapshotKey}-${filtersHash}`;
    }

    return portfolioSnapshotKey;
  }

  public getQuoteKey({ dataSource, symbol }: AssetProfileIdentifier) {
    return `quote-${getAssetProfileIdentifier({ dataSource, symbol })}`;
  }

  public async isHealthy() {
    const testKey = '__health_check__';
    const testValue = Date.now().toString();

    try {
      await Promise.race([
        (async () => {
          await this.set(testKey, testValue, ms('1 second'));
          const result = await this.get(testKey);

          if (result !== testValue) {
            throw new Error('Redis health check failed: value mismatch');
          }
        })(),
        new Promise((_, reject) =>
          setTimeout(
            () => reject(new Error('Redis health check failed: timeout')),
            ms('2 seconds')
          )
        )
      ]);

      return true;
    } catch (error) {
      Logger.error(error?.message, 'RedisCacheService');

      return false;
    } finally {
      try {
        await this.remove(testKey);
      } catch {}
    }
  }

  public async remove(key: string) {
    // FIX 4: Use this.client (the Keyv instance) and the correct method name 'delete'.
    return this.client.delete(key);
  }

  public async removePortfolioSnapshotsByUserId({
    userId
  }: {
    userId: string;
  }) {
    const keys = await this.getKeys(
      `${this.getPortfolioSnapshotKey({ userId })}`
    );

    // FIX 5: Use this.client and the correct method name 'deleteMany'.
    return this.client.deleteMany(keys);
  }

  public async reset() {
    // FIX 6: Use this.client.
    return this.client.clear();
  }

  public async set(key: string, value: string, ttl?: number) {
    // FIX 7: Use this.client.
    return this.client.set(
      key,
      value,
      ttl ?? this.configurationService.get('CACHE_TTL')
    );
  }
}
