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
import { MysqlModule } from "@/utils/mysql/mysql.module";
import { MySqlService } from "@/utils/mysql/mysql.service";
import { AggregatedAnalyticsModule } from "@/agregatedAnalytics/analytics.module";

@Module({
  imports: [
    AnalyticsClickHouseModule,
    AnalyticsModule,
    PrometheusPrometheusModule,
    AggregatedAnalyticsModule,
    ScheduleModule.forRoot(),
    MysqlModule,
    // CronModule,
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
    MySqlService,
  ],
  exports: [MySqlService],
})
export class AppModule {}
