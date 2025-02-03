import {
  Controller,
  Logger, Get, Inject
} from "@nestjs/common";
import * as dotenv from 'dotenv';
import { AggregatedAnalyticsService } from './analytics.service';


dotenv.config();

@Controller('aggregatedAnalytics')
export class AggregatedAnalyticsController {
  private readonly logger = new Logger(AggregatedAnalyticsController.name);
  constructor(
    private readonly analyticsService: AggregatedAnalyticsService,
) {}

  @Get()
  async getAnalytics(): Promise<any> {
    let startOfDay = new Date();
    startOfDay.setUTCFullYear(2025, 1, 2)
    console.log(`Total records for ${Number(process.env.REQUESTS)} records`);
    for (let i = 0; i < 24; i ++ ) {
      startOfDay.setUTCHours(i, 0, 0, 0);
      let datePerHour = startOfDay.toISOString().replace('T', ' ').split('.')[0]
      console.log(datePerHour)
      await this.analyticsService.generateAllDataPerHour(Number(process.env.DSP_SPEND), datePerHour);
    }
  }
}
