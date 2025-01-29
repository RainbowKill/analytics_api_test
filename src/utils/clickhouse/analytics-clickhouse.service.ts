import { Injectable, Inject } from '@nestjs/common';
import { ClickHouseClient } from '@clickhouse/client';

@Injectable()
export class AnalyticsClickHousesService {
  constructor(
    @Inject('ANALYTICS_CLICKHOUSE_CLIENT')
    private readonly AnalyticsClickHouseClient: ClickHouseClient,
  ) {}

  async checkConnection(): Promise<boolean> {
    try {
      await this.AnalyticsClickHouseClient.query({
        query: 'SELECT 1',
        format: 'JSONEachRow',
      });
      return true;
    } catch (error) {
      return false;
    }
  }
}
