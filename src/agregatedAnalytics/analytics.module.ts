import { Module } from '@nestjs/common';
import { AggregatedAnalyticsController } from './analytics.controller';
import { AggregatedAnalyticsService } from './analytics.service';
import { PrometheusPrometheusModule } from '@/utils/prometheus/prometheus.module';
import { MySqlService } from "@/utils/mysql/mysql.service";

@Module({
  imports: [PrometheusPrometheusModule],
  controllers: [AggregatedAnalyticsController],
  providers: [AggregatedAnalyticsService, MySqlService],
})
export class AggregatedAnalyticsModule {}
