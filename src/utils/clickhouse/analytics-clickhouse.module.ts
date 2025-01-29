import { Module, Global } from '@nestjs/common';
import { ClickHouseClient, createClient } from '@clickhouse/client';
import { AnalyticsClickhouseConfig } from '../../configs/dev.config';
import { AnalyticsClickHousesService } from './analytics-clickhouse.service';

const config = AnalyticsClickhouseConfig();

@Global()
@Module({
  providers: [
    {
      provide: 'ANALYTICS_CLICKHOUSE_CLIENT',
      useFactory: (): ClickHouseClient => {
        return createClient({
          url: config.host,
          database: config.database,
          username: config.username,
          password: config.password,
          request_timeout: 900000,
        });
      },
    },
    AnalyticsClickHousesService,
  ],
  exports: ['ANALYTICS_CLICKHOUSE_CLIENT', AnalyticsClickHousesService],
})
export class AnalyticsClickHouseModule {}
