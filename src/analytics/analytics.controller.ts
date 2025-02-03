import {
  Controller,
  Logger, Get, Inject
} from "@nestjs/common";
import * as dotenv from 'dotenv';
import { AnalyticsService } from './analytics.service';
import { ClickHouseClient } from "@clickhouse/client";
dotenv.config();

@Controller('analytics')
export class AnalyticsController {
  private readonly logger = new Logger(AnalyticsController.name);
  constructor(
    private readonly analyticsService: AnalyticsService,
    @Inject('ANALYTICS_CLICKHOUSE_CLIENT')
    private readonly AnalyticsClickHouseClient: ClickHouseClient
) {}

  @Get()
  async getAnalytics(): Promise<any> {
    let startOfDay = new Date();
    startOfDay.setUTCDate(1);
    console.log(`Total records for ${Number(process.env.REQUESTS)} records`);
    for (let i = 0; i < 24; i ++ ) {
      startOfDay.setUTCHours(i, 0, 0, 0);
      let datePerHour = startOfDay.toISOString().replace('T', ' ').split('.')[0]
      console.log(datePerHour)
      await this.analyticsService.generateAllDataPerHour(Number(process.env.REQUESTS), datePerHour);
    }
  }
}
