import { Module } from '@nestjs/common';
import { DataInsertionCommand } from './insert-data.command';
import { CronModule } from './../cron/crons.module';
import { PrometheusPrometheusModule } from '@/utils/prometheus/prometheus.module';

@Module({
  imports: [CronModule, PrometheusPrometheusModule],
  providers: [DataInsertionCommand],
})
export class CommandsModule {}
