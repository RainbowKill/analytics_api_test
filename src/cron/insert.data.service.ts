import { Injectable, HttpException, HttpStatus, Inject } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { ClickHouseClient } from '@clickhouse/client';
import { AnalyticsClickHousesService } from '@/utils/clickhouse/analytics-clickhouse.service';
import { aggregatedTables, dailyUpdateState } from '@/enums/tables';
import { generateHourlyAggregatedRequests } from '@/cron/queries/hourly_aggregated_requests';
import { generateHourlyAggregatedOurRequests } from '@/cron/queries/hourly_aggregated_our_requests';
import { generateHourlyAggregatedImpressions } from '@/cron/queries/hourly_aggregated_impressions';
import { PrometheusService } from '@/utils/prometheus/prometheus.service';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';
import { Logger } from 'winston';
import { startDay } from '@/constants';
import { databaseName } from '@/constants';

@Injectable()
export class InsertDataService {
  constructor(
    @Inject(WINSTON_MODULE_PROVIDER) private readonly logger: Logger,
    @Inject('ANALYTICS_CLICKHOUSE_CLIENT')
    private readonly AnalyticsClickHouseClient: ClickHouseClient,
    private readonly analyticsClickHousesService: AnalyticsClickHousesService,
    private readonly prometheusService: PrometheusService,
  ) {}

  @Cron('8 * * * *') // cron expression for daily at  midnight
  async handleCronREquests() {
    await this.runDataInsertion({
      aggregatedTableName: aggregatedTables.Requests,
      generateQueryFunction: generateHourlyAggregatedRequests,
    });
    this.prometheusService.inc('requests_crone');
  }

  @Cron('11 * * * *') // cron expression for daily at  midnight
  async handleCronImpressions() {
    await this.runDataInsertion({
      aggregatedTableName: aggregatedTables.Impressions,
      generateQueryFunction: generateHourlyAggregatedImpressions,
    });
    this.prometheusService.inc('impressions_crone');
  }

  @Cron('9 * * * *') // cron expression for daily at  midnight
  async handleCronOurRequests() {
    await this.runDataInsertion({
      aggregatedTableName: aggregatedTables.OurRequests,
      generateQueryFunction: generateHourlyAggregatedOurRequests,
    });
    this.prometheusService.inc('our_requests_crone');
  }

  // This function will run daily at midnight
  async runDataInsertion({
    aggregatedTableName,
    generateQueryFunction,
  }: {
    aggregatedTableName: string;
    generateQueryFunction: (data: {
      table: string;
      lastProcessedTime: string;
      lastProcessedTimePlusPeriod: string;
    }) => string;
  }) {
    this.logger.log({
      message: `Running data insertion for table: ${aggregatedTableName}`,
      level: 'info',
    });

    // Check ClickHouse connection
    if (!(await this.analyticsClickHousesService.checkConnection())) {
      const currentTime = new Date().toISOString();
      this.logger.log({
        message: `Analytics database connection failed at ${currentTime}`,
        level: 'error',
      });
      throw new HttpException(
        'Analytics database connection failed',
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }

    try {
      // Get the last processed time for the specific result table
      const lastProcessedTimeQuery = `
        SELECT last_processed_time
        FROM ${databaseName}.${dailyUpdateState}
        WHERE result_table_name = '${aggregatedTableName}';
      `;
      const lastProcessedTimeResult =
        await this.AnalyticsClickHouseClient.query({
          query: lastProcessedTimeQuery,
          format: 'JSONEachRow',
        });

      // Get the result from `lastProcessedTimeResult`
      const rows = await lastProcessedTimeResult.json();

      const lastProcessedTime = (rows[0] as any)?.last_processed_time; // Accessing the first row and specific field
      this.logger.log({
        message: `Last processed time retrieved successfully for table: ${aggregatedTableName}`,
        level: 'info',
      });

      if (!lastProcessedTime) {
        this.logger.log({
          message: 'Failed to retrieve last processed time',
          level: 'error',
        });
        throw new HttpException(
          'Failed to retrieve last processed time',
          HttpStatus.INTERNAL_SERVER_ERROR,
        );
      }

      function getStartOfCurrentHour() {
        // const now = new Date('2024-10-29T01:00:00Z');
        const now = new Date();
        const year = now.getUTCFullYear();
        const month = now.getUTCMonth();
        const day = now.getUTCDate();
        const hour = now.getUTCHours();

        // Create a new Date object at the start of the current hour in UTC
        const startOfCurrentHour = new Date(
          Date.UTC(year, month, day, hour, 0, 0, 0),
        );
        return startOfCurrentHour;
      }

      // Set currentProcessedTime to the start of the day (00:00:00) for today's date
      const startOfCurrentHour = getStartOfCurrentHour();

      let currentProcessedTime = new Date(`${lastProcessedTime}Z`);
      this.logger.log({
        message: `Last processed time: ${currentProcessedTime}`,
        level: 'info',
      });

      while (currentProcessedTime < startOfCurrentHour) {
        // Create a new date object for the next 12 hours
        const currentProcessedTimeNextPeriod = new Date(
          `${currentProcessedTime}`,
        );
        currentProcessedTimeNextPeriod.setHours(
          currentProcessedTimeNextPeriod.getHours() + 1,
        );

        const formattedDate =
          currentProcessedTime.toISOString().split('T')[0] +
          ' ' +
          currentProcessedTime.toISOString().split('T')[1].split(':')[0] +
          ':00:00';
        const formattedDate2 =
          currentProcessedTimeNextPeriod.toISOString().split('T')[0] +
          ' ' +
          currentProcessedTimeNextPeriod
            .toISOString()
            .split('T')[1]
            .split(':')[0] +
          ':00:00';
        const insertDataQuery = generateQueryFunction({
          table: aggregatedTableName,
          lastProcessedTime: formattedDate,
          lastProcessedTimePlusPeriod: formattedDate2,
        });

        let success = false;
        let retries = 0;
        const maxRetries = 5;

        while (!success && retries < maxRetries) {
          try {
            await this.AnalyticsClickHouseClient.query({
              query: insertDataQuery,
              format: 'JSONEachRow',
            });
            this.logger.log({
              message: `Data inserted successfully for date: ${formattedDate} - ${formattedDate2}`,
              level: 'info',
            });
            success = true;
          } catch (error) {
            retries++;
            this.logger.log({
              message: `Error inserting data for date: ${formattedDate} - ${formattedDate2}. Error: ${error}. Retry ${retries}/${maxRetries}`,
              level: 'error',
            });
            this.prometheusService.inc('data_insertion_error');
            if (retries >= maxRetries) {
              console.error(
                `Max retries reached for date: ${formattedDate} - ${formattedDate2}. Stopping script.`,
              );
              this.prometheusService.inc('data_insertion_total_error');
              process.exit(1); // Stop the script
            }
          }
        }
        let success2 = false;
        let retries2 = 0;
        const maxRetries2 = 5;
        const updateTimeQuery = `
          ALTER TABLE ${databaseName}.${dailyUpdateState}
          UPDATE last_processed_time = '${formattedDate2}'
          WHERE result_table_name = '${aggregatedTableName}';
        `;
        while (!success2 && retries2 < maxRetries2) {
          try {
            await this.AnalyticsClickHouseClient.query({
              query: updateTimeQuery,
              format: 'JSONEachRow',
            });
            this.logger.log({
              message: `Time updated successfully for table: ${aggregatedTableName} last updated time: ${formattedDate2}`,
              level: 'info',
            });
            success2 = true;
          } catch (error) {
            retries2++;
            this.logger.log({
              message: `Error updating last processed time for table: ${aggregatedTableName}. Retry ${retries2}/${maxRetries2}`,
              level: 'error',
            });
            this.prometheusService.inc('table_time_insertion_error');
            if (retries2 >= maxRetries2) {
              this.logger.log({
                message: `Max retries reached for updating last processed time for table: ${aggregatedTableName}. Stopping script.`,
                level: 'error',
              });
              this.prometheusService.inc('table_time_insertion_total_error');
              process.exit(1); // Stop the script
            }
          }
        }

        // Increment the currentProcessedTime by 12 hours
        currentProcessedTime = currentProcessedTimeNextPeriod;
      }
      this.logger.log({
        message: `Last processed time updated successfully for table: ${aggregatedTableName}`,
        level: 'info',
      });
    } catch (error) {
      this.logger.log({
        message: `Error inserting data: ${error}`,
        level: 'error',
      });
    }
  }

  // Create a public method to allow manual execution
  async runManually(params: {
    aggregatedTableName: string;
    generateQueryFunction: (data: {
      table: string;
      lastProcessedTime: string;
    }) => string;
  }) {
    this.logger.log({
      message: `Running data insertion manually for table: ${params.aggregatedTableName}`,
      level: 'info',
    });
    await this.runDataInsertion(params);
  }

  // Create a public method to allow manual execution with reset
  async runManuallyReset({
    aggregatedTableName,
    generateQueryFunction,
  }: {
    aggregatedTableName: string;
    generateQueryFunction: (data: {
      table: string;
      lastProcessedTime: string;
    }) => string;
  }) {
    const updateTimeQuery = `
        ALTER TABLE ${databaseName}.${dailyUpdateState}
        UPDATE last_processed_time = '${startDay}'
        WHERE result_table_name = '${aggregatedTableName}';
      `;
    this.logger.log({
      message: `Running data insertion manually with reset for table: ${aggregatedTableName}`,
      level: 'info',
    });
    await this.AnalyticsClickHouseClient.query({
      query: updateTimeQuery,
      format: 'JSONEachRow',
    });
    this.logger.log({
      message: `Data in ${databaseName}.daily_update_state reset successfully!`,
      level: 'info',
    });
    const truncateTableQuery = `
        TRUNCATE TABLE ${databaseName}.${aggregatedTableName};
      `;
    await this.AnalyticsClickHouseClient.query({
      query: truncateTableQuery,
      format: 'JSONEachRow',
    });
    this.logger.log({
      message: `Data in ${databaseName}.${aggregatedTableName} reset successfully!`,
      level: 'info',
    });
    await this.runDataInsertion({
      aggregatedTableName,
      generateQueryFunction,
    });
  }
}
