import { Module } from '@nestjs/common';
import { InsertDataService } from './insert.data.service';
import { PrometheusPrometheusModule } from '../utils/prometheus/prometheus.module';

@Module({
  imports: [PrometheusPrometheusModule],
  providers: [InsertDataService],
  exports: [InsertDataService],
})
export class CronModule {}
