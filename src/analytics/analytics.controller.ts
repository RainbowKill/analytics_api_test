import {
  Controller,
  Post,
  UsePipes,
  ValidationPipe,
  Res,
  Logger,
  Body,
} from '@nestjs/common';
import { AnalyticsService } from './analytics.service';

@Controller('analytics')
export class AnalyticsController {
  private readonly logger = new Logger(AnalyticsController.name); // Initialize Logger
  constructor(private readonly analyticsService: AnalyticsService) {}

  @Post('data')
  @UsePipes(new ValidationPipe())
  async getAnalytics(): Promise<any> {
    // return this.analyticsService.getAnalytics(body);
  }
}
