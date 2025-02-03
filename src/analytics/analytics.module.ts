import { Module } from '@nestjs/common';
import { AnalyticsController } from './analytics.controller';
import { AnalyticsService } from './analytics.service';
import { PrometheusPrometheusModule } from '@/utils/prometheus/prometheus.module';
import { MySqlService } from "@/utils/mysql/mysql.service";

@Module({
  imports: [PrometheusPrometheusModule],
  controllers: [AnalyticsController],
  providers: [AnalyticsService, MySqlService],
})
export class AnalyticsModule {}
