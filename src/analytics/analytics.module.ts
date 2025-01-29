import { Module } from '@nestjs/common';
import { AnalyticsController } from './analytics.controller';
import { AnalyticsService } from './analytics.service';
import { PrometheusPrometheusModule } from '@/utils/prometheus/prometheus.module';

@Module({
  imports: [PrometheusPrometheusModule],
  controllers: [AnalyticsController],
  providers: [AnalyticsService],
})
export class AnalyticsModule {}
