import { Module } from '@nestjs/common';
import { CommandModule } from 'nestjs-command';
import { APP_INTERCEPTOR } from '@nestjs/core';
import { AppController } from '@/app.controller';
import { AnalyticsModule } from '@/analytics/analytics.module';
import { AnalyticsClickHouseModule } from '@/utils/clickhouse/analytics-clickhouse.module';
import { LoggingInterceptor } from '@/logging.interceptor';
import { PrometheusPrometheusModule } from '@/utils/prometheus/prometheus.module';
import { ScheduleModule } from '@nestjs/schedule';
import { CronModule } from '@/cron/crons.module';
import { CommandsModule } from '@/commands/commands.module';
import { LoggerModule } from '@/utils/logger/logger.module';
import { CustomLoggerService } from '@/utils/logger/logger.service';
// @TODO on module init check clickhouse connection
@Module({
  imports: [
    AnalyticsClickHouseModule,
    AnalyticsModule,
    PrometheusPrometheusModule,
    ScheduleModule.forRoot(),
    CronModule,
    CommandModule,
    CommandsModule,
    LoggerModule,
  ],
  controllers: [AppController],
  providers: [
    {
      provide: APP_INTERCEPTOR,
      useClass: LoggingInterceptor,
    },
    {
      provide: 'LoggerService',
      useClass: CustomLoggerService,
    },
  ],
})
export class AppModule {}
