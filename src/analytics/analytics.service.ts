import { Injectable, Inject, HttpException, HttpStatus } from '@nestjs/common';
import { ClickHouseClient } from '@clickhouse/client';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';
import { Logger } from 'winston';
import { faker } from '@faker-js/faker';
import { MySqlService } from "@/utils/mysql/mysql.service";
import { dmaCodes, deviceModels } from "@/enums/statics";

let bidRequests = []
let ourBidRequests = []
let bidResponses = []
let Impressions = []
let Nurls = []
let Errors = []

console.warn = () => {}
const batch = 5
@Injectable()
export class AnalyticsService {

  constructor(
    @Inject(WINSTON_MODULE_PROVIDER) private readonly logger: Logger,
    @Inject('ANALYTICS_CLICKHOUSE_CLIENT')
    private readonly AnalyticsClickHouseClient: ClickHouseClient,
    private readonly mysqlService: MySqlService,
  ) {}

  async generateAllDataPerHour(totalRecords: number, eventTime: string) {

    console.log("DATA INSIDE FUNCTION" + eventTime);
    // Data from MySQL
    const supplyEndpointData: any[] = await this.mysqlService.query("SELECT id from supply_endpoints WHERE active = 1");
    const demandEndpointData: any[] = await this.mysqlService.query("SELECT id from demand_endpoints WHERE active = 1");
    const serverData: any[] = await this.mysqlService.query("SELECT ip from servers WHERE active = 1");
    const supplyIds: number[] = supplyEndpointData.map(row => row.id);
    const demandIds: number[] = demandEndpointData.map(row => row.id);
    const serverIps: string[] = serverData.map(row => row.ip);

    // Total records for bidRequest by hour
    const recordsPerHourForEveryDsp = Math.ceil((totalRecords / 24 ) / supplyIds.length);
    console.log(`Total records for bidRequest for ${recordsPerHourForEveryDsp} records for ${supplyIds.length} suppliers`);
    // Total records for ourBidRequest by hour
    const recordsPerHourForEveryDspAndSsp = Math.ceil(recordsPerHourForEveryDsp * 3 / demandIds.length);
    console.log(`Total records for ourBidRequest for ${recordsPerHourForEveryDspAndSsp} records for ${supplyIds.length} suppliers and for ${demandIds.length}`);
    // Total records for bidResponse by hour
    const recordsResponses = Math.ceil(recordsPerHourForEveryDspAndSsp / 9);
    console.log(`Total records for bidResponse for ${recordsResponses} records for ${supplyIds.length} suppliers and for ${demandIds.length}`);
    // Total records for Impressions by hour
    const recordsImpr = Math.ceil(recordsResponses / 2);
    console.log(`Total records for Impressions for ${recordsImpr} records for ${supplyIds.length} suppliers and for ${demandIds.length}`);
    // Total records for Nurls by hour
    const recordsNurls = Math.ceil(recordsImpr * 1.8);
    console.log(`Total records for Nurls for ${recordsNurls} records for ${supplyIds.length} suppliers and for ${demandIds.length}`);
    // Total records for Errors by hour
    const recordsErrors = {
      204: Math.ceil((recordsPerHourForEveryDspAndSsp - recordsResponses) * 0.9),
      400: Math.ceil((recordsPerHourForEveryDspAndSsp - recordsResponses) * 0.01),
      408: Math.ceil((recordsPerHourForEveryDspAndSsp - recordsResponses) * 0.02),
      422: Math.ceil((recordsPerHourForEveryDspAndSsp - recordsResponses) * 0.02),
      429: Math.ceil((recordsPerHourForEveryDspAndSsp - recordsResponses) * 0.02),
      500: Math.ceil((recordsPerHourForEveryDspAndSsp - recordsResponses) * 0.01),
      502: Math.ceil((recordsPerHourForEveryDspAndSsp - recordsResponses) * 0.02)
    }

    const tableDate = eventTime.split(" ")[0];
    const tableBidRequest = `v5_BidRequests_${tableDate.replaceAll("-", "_")}`;
    const tableOurBidRequests = `v5_OurBidRequests_${tableDate.replaceAll("-", "_")}`;
    const tableBidResponses = `v5_BidResponses_${tableDate.replaceAll("-", "_")}`;
    const tableImpressions = `v5_Impressions_${tableDate.replaceAll("-", "_")}`;
    const tableNurls = `v5_Nurls_${tableDate.replaceAll("-", "_")}`;
    const tableErrors = `v5_ErrorStatusNo200_${tableDate.replaceAll("-", "_")}`;


    // Logic generating fake analytics
    for (let i = 0; i < supplyIds.length; i++) {

      const generatedData = {
        bundle_domain: faker.number.int({ min: 1000000000, max: 9999999999 }) + "",
        size: faker.helpers.arrayElement(['320x50', '300x250', '728x90']),
        publisher_id: faker.string.uuid(),
        our_publisher_id: faker.string.uuid(),
        device_type: faker.number.int({ min: 1, max: 5 }),
        format: faker.helpers.arrayElement(['banner', 'video', 'native']),
        tmax: faker.number.int({ min: 50, max: 1500 }),
        geo_country: faker.address.countryCode(),
        crid: faker.string.uuid(),
        repacked_crid: faker.string.uuid(),
      }

      ///// bidRequests
      if(bidRequests.length > 10000) {
        try {
          await this.insertData(tableBidRequest, bidRequests)
          bidRequests = []
        } catch(error) {
          console.error(error);
        }
      } else {
        bidRequests.push(...this.generateBidRequest(recordsPerHourForEveryDsp, supplyIds[i], eventTime, serverIps, generatedData))
      }

      for (let j = 0; j < demandIds.length; j++) {

        ///// ourBidRequests
        if(ourBidRequests.length > 10000) {
        try {
          await this.insertData(tableOurBidRequests, ourBidRequests)
          ourBidRequests = []
          } catch(error) {
          console.error(error);
          }
        } else {
          ourBidRequests.push(...this.generateOurBidRequest(recordsPerHourForEveryDspAndSsp, supplyIds[i], demandIds[j], eventTime, serverIps, generatedData))
        }
        ////// bidResponses
        if(bidResponses.length > 10000) {
        try {
          await this.insertData(tableBidResponses, bidResponses)
          bidResponses = []
          } catch(error) {
          console.error(error);
          }
        } else {
          bidResponses.push(...this.generateBidResponse(recordsResponses, supplyIds[i], demandIds[j], eventTime, serverIps, generatedData))
        }

        ///// Impressions
        if(Impressions.length > 50000) {
        try {
          await this.insertData(tableImpressions, Impressions)
          Impressions = []
          } catch(error) {
          console.error(error);
          }
        } else {
          Impressions.push(...this.generateImpressions(recordsImpr, supplyIds[i], demandIds[j], eventTime, serverIps, generatedData))
        }

        ///// Nurls
        if(Nurls.length > 50000) {
        try {
          await this.insertData(tableNurls, Nurls)
          Nurls = []
          } catch(error) {
          console.error(error);
          }
        } else {
          Nurls.push(...this.generateNurl(recordsNurls, supplyIds[i], demandIds[j], eventTime, serverIps, generatedData))
        }

        ///// Errors
        if(Errors.length > 10000) {
        try {
          await this.insertData(tableErrors, Errors)
          Errors = []
          } catch(error) {
          console.error(error);
          }
        } else {
          Errors.push(...this.generateErrors(recordsErrors, supplyIds[i], demandIds[j], eventTime))
        }
      }
    }
    await this.insertData(tableBidRequest, bidRequests)
    await this.insertData(tableOurBidRequests, ourBidRequests)
    await this.insertData(tableBidResponses, bidResponses)
    await this.insertData(tableImpressions, Impressions)
    await this.insertData(tableNurls, Nurls)
    await this.insertData(tableErrors, Errors)
    bidRequests = []
    ourBidRequests = []
    bidResponses = []
    Impressions = []
    Nurls = []
    Errors = []
  }

  generateBidRequest(totalRecords: number, ssp_id: number, date: string, serverIps: string[], generatedData: object){

    const data = [];
    const batchCount = Math.ceil(totalRecords / batch);

    for (let i = 0; i < batch; i++) {

      data.push({
        event_time: date,
        bundle_domain: generatedData['bundle_domain'],
        device_type: generatedData['device_type'],
        device_os: faker.helpers.arrayElement(['ios', 'android', 'windows', 'macos', 'rokus', 'webos']),
        geo_country: generatedData['geo_country'],
        device_env: faker.helpers.arrayElement(["inapp", "web", "desktop"]),
        supply_bidfloor: faker.number.float({ min: 0.01, max: 2 }),
        format: generatedData['format'],
        tmax: generatedData['tmax'],
        size: generatedData['size'],
        rewarded_video: faker.number.int({ min: 0, max: 1 }),
        publisher_id: generatedData['publisher_id'],
        ssp_endpoint_id: ssp_id,
        pmp_flag: 0,
        auction_type: faker.number.int({ min: 0, max: 1 }),
        deal_id: faker.string.uuid(),
        deal_bidfloor: faker.number.float({ min: 0.01, max: 10 }),
        interstitial: faker.number.int({ min: 0, max: 1 }),
        device_model: faker.helpers.arrayElement(deviceModels),
        device_osv: faker.system.semver(),
        device_make: faker.company.name(),
        device_isp: faker.company.name(),
        device_connection: faker.number.int({ min: 1, max: 5 }),
        city: faker.address.city(),
        region: faker.address.state({abbreviated: true}),
        app_name: faker.company.name(),
        server: faker.helpers.arrayElement(serverIps),
        user_id: faker.string.uuid(),
        count: batchCount,
        ip: faker.internet.ip(),
        zip: faker.address.zipCode(),
        dma: faker.helpers.arrayElement(dmaCodes),
      });
    }
    return data
  }

   generateOurBidRequest(totalRecords: number, ssp_id: number, dsp_id: number, date: string, serverIps: string[], generatedData: object){

    const data = [];
    const batchCount = Math.ceil(totalRecords / batch);

      for (let i = 0; i < batch; i++) {

        data.push({
          event_time: date,
          bundle_domain: generatedData['bundle_domain'],
          device_type: generatedData['device_type'],
          device_os: faker.helpers.arrayElement(['ios', 'android', 'windows', 'macos', 'rokus', 'webos']),
          geo_country: generatedData['geo_country'],
          device_env: faker.helpers.arrayElement(["inapp", "web", "desktop"]),
          supply_bidfloor: faker.number.float({ min: 0.01, max: 2}),
          format: generatedData['format'],
          tmax: generatedData['tmax'],
          size: generatedData['size'],
          rewarded_video: faker.number.int({ min: 0, max: 1 }),
          publisher_id: generatedData['publisher_id'],
          ssp_endpoint_id: ssp_id,
          dsp_endpoint_id: dsp_id,
          pmp_flag: faker.number.int({ min: 0, max: 1 }),
          auction_type: faker.number.int({ min: 0, max: 1 }),
          deal_id: faker.string.uuid(),
          deal_bidfloor: faker.number.float({ min: 0.01, max: 10}),
          interstitial: faker.number.int({ min: 0, max: 1 }),
          device_model: faker.helpers.arrayElement(deviceModels),
          device_osv: faker.system.semver(),
          device_make: faker.company.name(),
          device_isp: faker.company.name(),
          demand_bidfloor: faker.number.float({ min: 0.01, max: 10}),
          device_connection: faker.number.int({ min: 1, max: 5 }),
          city: faker.address.city(),
          region: faker.address.state({abbreviated: true}),
          app_name: faker.company.name(),
          server: faker.helpers.arrayElement(serverIps),
          our_publisher_id: generatedData['our_publisher_id'],
          our_tmax: generatedData['tmax'],
          count: batchCount,
          user_id: faker.string.uuid(),
          ip: faker.internet.ip(),
          zip: faker.location.zipCode(),
          dma: faker.helpers.arrayElement(dmaCodes),
        });
      }
      return data
  }

   generateBidResponse(totalRecords: number, ssp_id: number, dsp_id: number, date: string, serverIps: string[], generatedData: object){
    const data = [];
    const batchCount = Math.ceil(totalRecords / batch);

      for (let i = 0; i < batch; i++) {

        data.push({
          event_time: date,
          bundle_domain: generatedData['bundle_domain'],
          device_type: generatedData['device_type'],
          device_os: faker.helpers.arrayElement(['ios', 'android', 'windows', 'macos', 'rokus', 'webos']),
          geo_country: generatedData['geo_country'],
          device_env: faker.helpers.arrayElement(["inapp", "web", "desktop"]),
          supply_bidfloor: faker.number.float({ min: 0.01, max: 2}),
          format: generatedData['format'],
          tmax: generatedData['tmax'],
          size: generatedData['size'],
          rewarded_video: faker.number.int({ min: 0, max: 1 }),
          publisher_id: generatedData['publisher_id'],
          ssp_endpoint_id: ssp_id,
          dsp_endpoint_id: dsp_id,
          price: faker.number.float({ min: 0.01, max: 10}),
          crid: generatedData['crid'],
          cid: faker.string.uuid(),
          pmp_flag: faker.number.int({ min: 0, max: 1 }),
          auction_type: faker.number.int({ min: 0, max: 1 }),
          deal_id: faker.string.uuid(),
          deal_bidfloor: faker.number.float({ min: 0.01, max: 10}),
          interstitial: faker.number.int({ min: 0, max: 1 }),
          device_model: faker.helpers.arrayElement(deviceModels),
          device_osv: faker.system.semver(),
          device_make: faker.company.name(),
          device_isp: faker.company.name(),
          demand_bidfloor: faker.number.float({ min: 0.01, max: 10}),
          device_connection: faker.number.int({ min: 1, max: 5 }),
          city: faker.location.city(),
          region: faker.location.state(),
          app_name: faker.company.name(),
          server: faker.helpers.arrayElement(serverIps),
          our_publisher_id: generatedData['our_publisher_id'],
          our_tmax: generatedData['tmax'],
          count: batchCount,
          user_id: faker.string.uuid(),
          ip: faker.internet.ip(),
          zip: faker.location.zipCode(),
          dma: faker.helpers.arrayElement(dmaCodes),
        });
      }
      return data
  }


  generateImpressions(totalRecords: number, ssp_id: number, dsp_id: number, date: string, serverIps: string[], generatedData: object){

  const data = [];
  // const batchCount = Math.ceil(totalRecords / batch);

    for (let i = 0; i < totalRecords; i++) {

      const dspSpend = faker.number.float({ min: 0.0005, max: 0.00052})

      data.push({
        event_time: date,
        bundle_domain: generatedData['bundle_domain'],
        device_type: generatedData['device_type'],
        device_os: faker.helpers.arrayElement(['ios', 'android', 'windows', 'macos', 'rokus', 'webos']),
        geo_country: generatedData['geo_country'],
        device_env: faker.helpers.arrayElement(["inapp", "web", "desktop"]),
        supply_bidfloor: faker.number.float({ min: 0.01, max: 2}),
        format: generatedData['format'],
        tmax: generatedData['tmax'],
        size: generatedData['size'],
        rewarded_video: faker.number.int({ min: 0, max: 1 }),
        publisher_id: generatedData['publisher_id'],
        ssp_endpoint_id: ssp_id,
        dsp_endpoint_id: dsp_id,
        dsp_spend: dspSpend,
        ssp_spend: faker.number.float({ min: dspSpend - dspSpend * 0.3, max: dspSpend - dspSpend * 0.25}),
        price: faker.number.float({ min: 0.01, max: 2}),
        crid: generatedData['crid'],
        cid: faker.string.uuid(),
        pmp_flag: faker.number.int({ min: 0, max: 1 }),
        auction_type: faker.number.int({ min: 1, max: 2 }),
        deal_id: faker.string.uuid(),
        deal_bidfloor: faker.number.float({ min: 0.01, max: 10}),
        interstitial: faker.number.int({ min: 0, max: 1 }),
        device_model: faker.helpers.arrayElement(deviceModels),
        device_osv: faker.system.semver(),
        device_make: faker.company.name(),
        device_isp: faker.company.name(),
        demand_bidfloor: faker.number.float({ min: 0.01, max: 10}),
        device_connection: faker.number.int({ min: 1, max: 5 }),
        city: faker.location.city(),
        region: faker.location.state(),
        app_name: faker.company.name(),
        repack_crid: generatedData['repack_crid'],
        server: faker.helpers.arrayElement(serverIps),
        our_publisher_id: generatedData['our_publisher_id'],
        our_tmax: generatedData['tmax'],
        count: 1,
        user_id: faker.string.uuid(),
        ip: faker.internet.ip(),
        zip: faker.location.zipCode(),
        dma: faker.helpers.arrayElement(dmaCodes),
      });
    }
    return data
}

generateNurl(totalRecords: number, ssp_id: number, dsp_id: number, date: string, serverIps: string[], generatedData: object){

  const data = [];
  // const batchCount = Math.ceil(totalRecords / batch);

    for (let i = 0; i < totalRecords; i++) {

      const dspSpend = faker.number.float({ min: 0.0005, max: 0.00052})


      data.push({
        event_time: date,
        bundle_domain: generatedData['bundle_domain'],
        device_type: generatedData['device_type'],
        device_os: faker.helpers.arrayElement(['iOS', 'Android', 'Windows', 'MacOS', 'Linux']),
        geo_country: generatedData['geo_country'],
        device_env: faker.helpers.arrayElement(["inapp", "web", "desktop"]),
        supply_bidfloor: faker.number.float({ min: 0.01, max: 2}),
        format: generatedData['format'],
        tmax: generatedData['tmax'],
        size: generatedData['size'],
        rewarded_video: faker.number.int({ min: 0, max: 1 }),
        publisher_id: generatedData['publisher_id'],
        ssp_endpoint_id: ssp_id,
        dsp_endpoint_id: dsp_id,
        dsp_spend: dspSpend,
        ssp_spend: faker.number.float({ min: dspSpend - dspSpend * 0.3, max: dspSpend - dspSpend * 0.25}),
        price: faker.number.float({ min: 0.01, max: 10}),
        crid: generatedData['crid'],
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
        repack_crid: generatedData['repack_crid'],
        server: faker.helpers.arrayElement(serverIps),
        our_publisher_id: generatedData['our_publisher_id'],
        our_tmax: generatedData['tmax'],
        count: 1,
        user_id: faker.string.uuid(),
        ip: faker.internet.ip(),
        zip: faker.location.zipCode(),
        dma: faker.helpers.arrayElement(dmaCodes),
      });
    }
    return data
}

generateBurl(totalRecords: number, ssp_id: number, dsp_id: number, date: string, serverIps: string[], generatedData: object){

  const data = [];
  // const batchCount = Math.ceil(totalRecords / batch);

    for (let i = 0; i < totalRecords; i++) {

      const dspSpend = faker.number.float({ min: 0.001, max: 0.0012})


      data.push({
        event_time: date,
        bundle_domain: generatedData['bundle_domain'],
        device_type: generatedData['device_type'],
        device_os: faker.helpers.arrayElement(['iOS', 'Android', 'Windows', 'MacOS', 'Linux']),
        geo_country: generatedData['geo_country'],
        device_env: faker.helpers.arrayElement(["inapp", "web", "desktop"]),
        supply_bidfloor: faker.number.float({ min: 0.01, max: 2}),
        format: generatedData['format'],
        tmax: generatedData['tmax'],
        size: generatedData['size'],
        rewarded_video: faker.number.int({ min: 0, max: 1 }),
        publisher_id: generatedData['publisher_id'],
        ssp_endpoint_id: ssp_id,
        dsp_endpoint_id: dsp_id,
        dsp_spend: dspSpend,
        ssp_spend: faker.number.float({ min: dspSpend - dspSpend * 0.3, max: dspSpend - dspSpend * 0.25}),
        price: faker.number.float({ min: 0.01, max: 10}),
        crid: generatedData['crid'],
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
        repack_crid: generatedData['repack_crid'],
        server: faker.helpers.arrayElement(serverIps),
        our_publisher_id: generatedData['our_publisher_id'],
        our_tmax: generatedData['tmax'],
        burl: faker.internet.url() ,
        count: 1,
        user_id: faker.string.uuid(),
        ip: faker.internet.ip(),
        zip: faker.location.zipCode(),
        dma: faker.helpers.arrayElement(dmaCodes),
      });
    }
    return data
}

generateErrors(statusCodesCount: object, ssp_id: number, dsp_id: number, date: string){

  const data = [];

  function pushErrors(statuscode: number, statusCodesCount: object) {
    data.push({
      YMDH: date,
      dsp_endpoint_id: dsp_id,
      status_code: statuscode,
      ssp_id: ssp_id,
      count: statusCodesCount[statuscode],
    });
  }
  pushErrors(204, statusCodesCount)
  pushErrors(400, statusCodesCount)
  pushErrors(408, statusCodesCount)
  pushErrors(422, statusCodesCount)
  pushErrors(429, statusCodesCount)
  pushErrors(500, statusCodesCount)
  pushErrors(502, statusCodesCount)

  return data
}

  async insertData(table: string, data: any[]): Promise<void> {

    let attempt = 0;
    let maxRetries = 3;
    let delay: number = 1000

    while (attempt < maxRetries) {
      try {
        console.log(`Try to insert data #${attempt + 1}`);
        await this.AnalyticsClickHouseClient.insert({
          table,
          values: data,
          format: 'JSONEachRow',
        });
        return;
      } catch (error) {
        console.error(`Error inserting to clickhouse: ${error.message}`);
        attempt++;
        if (attempt < maxRetries) {
          console.log(`Retry after ${delay} ms...`);
          await new Promise((resolve) => setTimeout(resolve, delay));
          delay *= 2;
        } else {
          console.error('Number of retries is over');
        }
      }
    }

  }
}
