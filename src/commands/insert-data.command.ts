import { Command } from 'nestjs-command';
import { Injectable } from '@nestjs/common';
import { Logger } from '@nestjs/common';
import { InsertDataService } from '@/cron/insert.data.service';
import { aggregatedTables } from '@/enums/tables';
import { generateHourlyAggregatedRequests } from '@/cron/queries/hourly_aggregated_requests';
import { generateHourlyAggregatedOurRequests } from '@/cron/queries/hourly_aggregated_our_requests';
import { generateHourlyAggregatedImpressions } from '@/cron/queries/hourly_aggregated_impressions';
import { PrometheusService } from '@/utils/prometheus/prometheus.service';
@Injectable()
export class DataInsertionCommand {
  constructor(
    private readonly insertDataService: InsertDataService,
    private readonly prometheusService: PrometheusService,
  ) {}
  private readonly logger = new Logger(DataInsertionCommand.name);

  // insert data manually, populate with new data from time in daily_update_state before today
  // @TODO block command if it's already running
  // npx nestjs-command run:data-insertion-requests
  @Command({
    command: 'run:data-insertion-requests',
    describe: 'Run data insertion',
  })
  async run() {
    await this.insertDataService.runManually({
      aggregatedTableName: aggregatedTables.Requests,
      generateQueryFunction: generateHourlyAggregatedRequests,
    });
    this.prometheusService.inc('run:data-insertion-requests');
  }

  // reset last update table time in daily_update_state, TRUNCATE TABLE, populate with new data before today
  // npx nestjs-command run:data-insertion-requests-reset
  @Command({
    command: 'run:data-insertion-requests-reset',
    describe: 'Run data insertion reset requests',
  })
  async runReset() {
    await this.insertDataService.runManuallyReset({
      aggregatedTableName: aggregatedTables.Requests,
      generateQueryFunction: generateHourlyAggregatedRequests,
    });
    this.prometheusService.inc('run:data-insertion-requests-reset');
  }

  // npx nestjs-command run:data-insertion-our-requests
  @Command({
    command: 'run:data-insertion-our-requests',
    describe: 'Run data insertion our requests',
  })
  async runSspDsp() {
    await this.insertDataService.runManually({
      aggregatedTableName: aggregatedTables.OurRequests,
      generateQueryFunction: generateHourlyAggregatedOurRequests,
    });
    this.prometheusService.inc('run:data-insertion-our-requests');
  }

  // npx nestjs-command run:data-insertion-reset-ssp-dsp
  @Command({
    command: 'run:data-insertion-our-requests-reset',
    describe: 'Run data insertion RESET our requests',
  })
  async runResetSspDsp() {
    await this.insertDataService.runManuallyReset({
      aggregatedTableName: aggregatedTables.OurRequests,
      generateQueryFunction: generateHourlyAggregatedOurRequests,
    });
    this.prometheusService.inc('run:data-insertion-our-requests-reset');
  }

  // npx nestjs-command run:data-insertion-impressions
  @Command({
    command: 'run:data-insertion-impressions',
    describe: 'Run data insertion impressions',
  })
  async runImpressions() {
    await this.insertDataService.runManually({
      aggregatedTableName: aggregatedTables.Impressions,
      generateQueryFunction: generateHourlyAggregatedImpressions,
    });
    this.prometheusService.inc('run:data-insertion-impressions');
  }

  // npx nestjs-command run:data-insertion-impressions-reset
  @Command({
    command: 'run:data-insertion-impressions-reset',
    describe: 'Run data insertion RESET impressions',
  })
  async runResetImpressions() {
    await this.insertDataService.runManuallyReset({
      aggregatedTableName: aggregatedTables.Impressions,
      generateQueryFunction: generateHourlyAggregatedImpressions,
    });
    this.prometheusService.inc('run:data-insertion-impressions-reset');
  }
}
