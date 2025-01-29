import { Injectable, Inject, HttpException, HttpStatus } from '@nestjs/common';
import { ClickHouseClient } from '@clickhouse/client';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';
import { Logger } from 'winston';
import { faker } from '@faker-js/faker';

@Injectable()
export class AnalyticsService {

  constructor(
    @Inject(WINSTON_MODULE_PROVIDER) private readonly logger: Logger,
    @Inject('ANALYTICS_CLICKHOUSE_CLIENT')
    private readonly AnalyticsClickHouseClient: ClickHouseClient,
  ) {}

  async generateBidRequest(totalRecords: number): Promise<any> {
      const tableName = 'v5_BidRequests_2025_01_20';

    const startOfDay = new Date();
    startOfDay.setHours(0, 0, 0, 0);

    const recordsPerHour = Math.floor(totalRecords / 24); // Сколько записей в час
    const data = [];

    for (let hour = 0; hour < 24; hour++) {
      for (let i = 0; i < recordsPerHour; i++) {
        const eventTime = new Date(startOfDay);
        eventTime.setHours(hour);
        eventTime.setMinutes(faker.number.int({ min: 0, max: 59 }));
        eventTime.setSeconds(faker.number.int({ min: 0, max: 59 }));

      data.push({
        event_time: eventTime.toISOString().replace('T', ' ').split('.')[0],
        bundle_domain: faker.number.int({ min: 1000000000, max: 9999999999 }),
        device_type: faker.number.int({ min: 1, max: 5 }),
        device_os: faker.helpers.arrayElement(['ios', 'android', 'windows', 'macos', 'rokus', 'webos']),
        geo_country: faker.address.countryCode(),
        device_env: faker.helpers.arrayElement(['app', 'web']),
        supply_bidfloor: faker.number.float({ min: 0.01, max: 10 }),
        format: faker.helpers.arrayElement(['banner', 'video', 'native']),
        tmax: faker.number.int({ min: 50, max: 1000 }),
        size: faker.helpers.arrayElement(['320x50', '300x250', '728x90']),
        rewarded_video: faker.number.int({ min: 0, max: 1 }),
        publisher_id: faker.string.uuid(),
        ssp_endpoint_id: faker.number.int({ min: 1, max: 100 }),
        pmp_flag: faker.number.int({ min: 0, max: 1 }),
        auction_type: faker.number.int({ min: 1, max: 2 }),
        deal_id: faker.string.uuid(),
        deal_bidfloor: faker.number.float({ min: 0.01, max: 10 }),
        interstitial: faker.number.int({ min: 0, max: 1 }),
        device_model: faker.commerce.productName(),
        device_osv: faker.system.semver(),
        device_make: faker.company.name(),
        device_isp: faker.company.name(),
        device_connection: faker.number.int({ min: 1, max: 5 }),
        city: faker.address.city(),
        region: faker.address.state(),
        app_name: faker.company.name(),
        server: faker.internet.ip(),
        user_id: faker.string.uuid(),
        count: faker.number.int({ min: 1, max: 5 }),
        ip: faker.internet.ip(),
        zip: faker.address.zipCode(),
        dma: faker.string.uuid(),
        });
      }

      await this.insertData(tableName, data);
    }
  }

   async generateOurBidRequest(totalRecords: number): Promise<void> {
    const tableName = 'v5_OurBidRequests_2025_01_29';

    const startOfDay = new Date();
    startOfDay.setHours(0, 0, 0, 0);

    const recordsPerHour = Math.floor(totalRecords / 24);
    const data = [];

    for (let hour = 0; hour < 24; hour++) {
      for (let i = 0; i < recordsPerHour; i++) {
        const eventTime = new Date(startOfDay);
        eventTime.setHours(hour);
        eventTime.setMinutes(faker.number.int({ min: 0, max: 59 }));
        eventTime.setSeconds(faker.number.int({ min: 0, max: 59 }));

        data.push({
          event_time: eventTime.toISOString().replace('T', ' ').split('.')[0],
          bundle_domain: faker.internet.domainName(),
          device_type: faker.number.int({ min: 1, max: 5 }),
          device_os: faker.helpers.arrayElement(['iOS', 'Android', 'Windows', 'MacOS', 'Linux']),
          geo_country: faker.location.countryCode('alpha-2'),
          device_env: faker.helpers.arrayElement(['app', 'web']),
          supply_bidfloor: faker.number.float({ min: 0.01, max: 10}),
          format: faker.helpers.arrayElement(['banner', 'video', 'native']),
          tmax: faker.number.int({ min: 100, max: 1000 }),
          size: faker.helpers.arrayElement(['320x50', '300x250', '728x90']),
          rewarded_video: faker.number.int({ min: 0, max: 1 }),
          publisher_id: faker.string.uuid(),
          ssp_endpoint_id: faker.number.int({ min: 1, max: 100 }),
          dsp_endpoint_id: faker.number.int({ min: 1, max: 100 }),
          pmp_flag: faker.number.int({ min: 0, max: 1 }),
          auction_type: faker.number.int({ min: 1, max: 2 }),
          deal_id: faker.string.uuid(),
          deal_bidfloor: faker.number.float({ min: 0.01, max: 10}),
          interstitial: faker.number.int({ min: 0, max: 1 }),
          device_model: faker.commerce.productName(),
          device_osv: faker.system.semver(),
          device_make: faker.company.name(),
          device_isp: faker.company.name(),
          demand_bidfloor: faker.number.float({ min: 0.01, max: 10}),
          device_connection: faker.number.int({ min: 1, max: 5 }),
          city: faker.location.city(),
          region: faker.location.state(),
          app_name: faker.company.name(),
          server: faker.internet.ip(),
          our_publisher_id: faker.string.uuid(),
          our_tmax: faker.number.int({ min: 100, max: 1000 }),
          count: faker.number.int({ min: 1, max: 5 }),
          user_id: faker.string.uuid(),
          ip: faker.internet.ip(),
          zip: faker.location.zipCode(),
          dma: faker.string.uuid(),
        });
      }
    }

    await this.insertData(tableName, data);
  }

   async generateBidResponse(totalRecords: number): Promise<void> {
    const tableName = 'v5_BidResponses_2025_01_15';

    const startOfDay = new Date();
    startOfDay.setHours(0, 0, 0, 0);

    const recordsPerHour = Math.floor(totalRecords / 24);
    const data = [];

    for (let hour = 0; hour < 24; hour++) {
      for (let i = 0; i < recordsPerHour; i++) {
        const eventTime = new Date(startOfDay);
        eventTime.setHours(hour);
        eventTime.setMinutes(faker.number.int({ min: 0, max: 59 }));
        eventTime.setSeconds(faker.number.int({ min: 0, max: 59 }));

        data.push({
          event_time: eventTime.toISOString().replace('T', ' ').split('.')[0],
          bundle_domain: faker.internet.domainName(),
          device_type: faker.number.int({ min: 1, max: 5 }),
          device_os: faker.helpers.arrayElement(['iOS', 'Android', 'Windows', 'MacOS', 'Linux']),
          geo_country: faker.location.countryCode('alpha-2'),
          device_env: faker.helpers.arrayElement(['app', 'web']),
          supply_bidfloor: faker.number.float({ min: 0.01, max: 10}),
          format: faker.helpers.arrayElement(['banner', 'video', 'native']),
          tmax: faker.number.int({ min: 100, max: 1000 }),
          size: faker.helpers.arrayElement(['320x50', '300x250', '728x90']),
          rewarded_video: faker.number.int({ min: 0, max: 1 }),
          publisher_id: faker.string.uuid(),
          ssp_endpoint_id: faker.number.int({ min: 1, max: 100 }),
          dsp_endpoint_id: faker.number.int({ min: 1, max: 100 }),
          price: faker.number.float({ min: 0.01, max: 10}),
          crid: faker.string.uuid(),
          cid: faker.string.uuid(),
          pmp_flag: faker.number.int({ min: 0, max: 1 }),
          auction_type: faker.number.int({ min: 1, max: 2 }),
          deal_id: faker.string.uuid(),
          deal_bidfloor: faker.number.float({ min: 0.01, max: 10}),
          interstitial: faker.number.int({ min: 0, max: 1 }),
          device_model: faker.commerce.productName(),
          device_osv: faker.system.semver(),
          device_make: faker.company.name(),
          device_isp: faker.company.name(),
          demand_bidfloor: faker.number.float({ min: 0.01, max: 10}),
          device_connection: faker.number.int({ min: 1, max: 5 }),
          city: faker.location.city(),
          region: faker.location.state(),
          app_name: faker.company.name(),
          server: faker.internet.ip(),
          our_publisher_id: faker.string.uuid(),
          our_tmax: faker.number.int({ min: 100, max: 1000 }),
          count: faker.number.int({ min: 1, max: 5 }),
          user_id: faker.string.uuid(),
          ip: faker.internet.ip(),
          zip: faker.location.zipCode(),
          dma: faker.string.uuid(),
        });
      }
    }

    await this.insertData(tableName, data);
  }


  async insertData(table: string, data: any[]): Promise<void> {
    await this.AnalyticsClickHouseClient.insert({
      table,
      values: data,
      format: 'JSONEachRow',
    });

    this.logger.log({level: 'info' , message: `Inserted ${data.length} data rows into ${table}`})
  }
}
