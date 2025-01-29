import { Controller, Get } from '@nestjs/common';
import { AnalyticsClickHousesService } from './utils/clickhouse/analytics-clickhouse.service';
@Controller()
export class AppController {
  constructor(
    private readonly analyticsClickHousesService: AnalyticsClickHousesService,
  ) {}

  // @TODO use REST API return same structure status, data, errors
  @Get('health')
  async healthCheck(): Promise<{
    status: string;
    analyticsDbConnection: boolean;
  }> {
    return {
      status: 'ok',
      analyticsDbConnection:
        await this.analyticsClickHousesService.checkConnection(),
    };
  }
}
