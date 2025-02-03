import { Injectable, Inject, HttpException, HttpStatus } from '@nestjs/common';
import { ClickHouseClient } from '@clickhouse/client';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';
import { Logger } from 'winston';
import { faker } from '@faker-js/faker';
import { MySqlService } from "@/utils/mysql/mysql.service";


let bidRequests = []
let ourBidRequests = []
let Impressions = []
let Errors = []

console.warn = () => {}
@Injectable()
export class AggregatedAnalyticsService {

  constructor(
    @Inject(WINSTON_MODULE_PROVIDER) private readonly logger: Logger,
    @Inject('ANALYTICS_CLICKHOUSE_CLIENT')
    private readonly AnalyticsClickHouseClient: ClickHouseClient,
    private readonly mysqlService: MySqlService,
  ) {}

  async generateAllDataPerHour(totalDspSpend: number, eventTime: string) {

    console.log("DATA INSIDE FUNCTION" + eventTime);
    // Data from MySQL
    const supplyEndpointData: any[] = await this.mysqlService.query("SELECT id from supply_endpoints WHERE active = 1");
    const demandEndpointData: any[] = await this.mysqlService.query("SELECT id from demand_endpoints WHERE active = 1");
    const serverData: any[] = await this.mysqlService.query("SELECT ip from servers WHERE active = 1");
    const supplyIds: number[] = supplyEndpointData.map(row => row.id);
    const demandIds: number[] = demandEndpointData.map(row => row.id);
    const serverIps: string[] = serverData.map(row => row.ip);

    const numberLines = faker.number.int({ min: 5, max: 40 });

    const dspSpend = faker.number.float({ min: 0.0005, max: 0.001 });

    const sspSpend = dspSpend * faker.number.float({ min: 0.7, max: 0.74 });

    const numberImpr = Math.ceil( totalDspSpend / 24 / supplyIds.length / demandIds.length / dspSpend / numberLines)

    const numberNurl = Math.ceil( numberImpr * 0.8 )

    const numberResp = Math.ceil(numberImpr / faker.number.float({ min: 0.45, max: 0.5 }) / 0.8)

    const numberOurReq = Math.ceil(numberResp / faker.number.float({ min: 0.1, max: 0.15 }))

    const numberReq = Math.ceil(numberOurReq * faker.number.float({ min: 1.1, max: 1.5 }))

    const totalRecordsObject = {
      bidRequests: numberReq,
      ourBidRequests: numberOurReq,
      bidResponses: numberResp,
      Impressions: numberImpr,
      recordsNurls: numberNurl,
      dspSpend: dspSpend,
      sspSpend: sspSpend,
    }
    // Total records for Errors by hour
    const recordsErrors = {
      204: Math.ceil((numberOurReq - numberResp) * 0.9),
      400: Math.ceil((numberOurReq - numberResp) * 0.01),
      408: Math.ceil((numberOurReq - numberResp) * 0.02),
      422: Math.ceil((numberOurReq - numberResp) * 0.02),
      429: Math.ceil((numberOurReq - numberResp) * 0.02),
      500: Math.ceil((numberOurReq - numberResp) * 0.01),
      502: Math.ceil((numberOurReq - numberResp) * 0.02)
    }

    // Table names
    const tableDate = eventTime.split(" ")[0];
    const tableBidRequest = `hourly_aggregated_requests_v3`;
    const tableOurBidRequests = `hourly_aggregated_our_requests_v3`;
    const tableImpressions = `hourly_aggregated_impressions_v3`;
    const tableErrors = `v5_ErrorStatusNo200_${tableDate.replaceAll("-", "_")}`;


    // Logic generating fake analytics
    for (let i = 0; i < supplyIds.length; i++) {
      for (let j = 0; j < demandIds.length; j++) {

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

        for( let a = 0; a < numberLines; a++) {

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
            bidRequests.push(...this.generateBidRequest(totalRecordsObject, supplyIds[i], eventTime, serverIps, generatedData))
          }

          ///// ourBidRequests
          if(ourBidRequests.length > 10000) {
          try {
            await this.insertData(tableOurBidRequests, ourBidRequests)
            ourBidRequests = []
            } catch(error) {
            console.error(error);
            }
          } else {
            ourBidRequests.push(...this.generateOurBidRequest(totalRecordsObject, supplyIds[i], demandIds[j], eventTime, serverIps, generatedData))
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
            Impressions.push(...this.generateImpressions(totalRecordsObject, supplyIds[i], demandIds[j], eventTime, serverIps, generatedData))
          }
        }
      }
    }
    await this.insertData(tableBidRequest, bidRequests)
    await this.insertData(tableOurBidRequests, ourBidRequests)
    await this.insertData(tableImpressions, Impressions)
    await this.insertData(tableErrors, Errors)
    bidRequests = []
    ourBidRequests = []
    Impressions = []
    Errors = []
  }

  generateBidRequest(totalRecords: object, ssp_id: number, date: string, serverIps: string[], generatedData: object){

    const data = [];

      data.push({
        time: date,
        ssp_endpoint_id: ssp_id,
        bundle_domain: generatedData['bundle_domain'],
        size: generatedData['size'],
        publisher_id: generatedData['publisher_id'],
        device_type: generatedData['device_type'],
        format: generatedData['format'],
        tmax: generatedData['tmax'],
        geo_country: generatedData['geo_country'],
        incomingRequests: totalRecords['bidRequests'],
        broadcastRequests: totalRecords['ourBidRequests'],
        receivedResponses: totalRecords['bidResponses'],
        impressions: totalRecords['Impressions'],
        burls:0,
        nurls: totalRecords["recordsNurls"],
        ssp_spend: totalRecords["sspSpend"] * totalRecords["Impressions"],
        dsp_spend: totalRecords["dspSpend"] * totalRecords["Impressions"],
        demand_bidfloor: faker.number.float({ min: 0.01, max: 1.1 })
      });
    return data
  }

   generateOurBidRequest(totalRecords: object, ssp_id: number, dsp_id: number, date: string, serverIps: string[], generatedData: object){

    const data = [];

        data.push({
          time: date,
          ssp_endpoint_id: ssp_id,
          dsp_endpoint_id: dsp_id,
          bundle_domain: generatedData['bundle_domain'],
          size: generatedData['size'],
          publisher_id: generatedData['publisher_id'],
          our_publisher_id: generatedData['our_publisher_id'],
          device_type: generatedData['device_type'],
          format: generatedData['format'],
          tmax: generatedData['tmax'],
          geo_country: generatedData['geo_country'],
          broadcastRequests: totalRecords['ourBidRequests'],
          receivedResponses: totalRecords['bidResponses'],
          impressions: totalRecords['Impressions'],
          burls:0,
          nurls: totalRecords["recordsNurls"],
          ssp_spend: totalRecords["sspSpend"] * totalRecords["Impressions"],
          dsp_spend: totalRecords["dspSpend"] * totalRecords["Impressions"],
          demand_bidfloor: faker.number.float({ min: 0.01, max: 1.1 })
        });

      return data
  }

  generateImpressions(totalRecords: object, ssp_id: number, dsp_id: number, date: string, serverIps: string[], generatedData: object){

  const data = [];


     data.push({
          time: date,
          ssp_endpoint_id: ssp_id,
          dsp_endpoint_id: dsp_id,
          bundle_domain: generatedData['bundle_domain'],
          size: generatedData['size'],
          publisher_id: generatedData['publisher_id'],
          our_publisher_id: generatedData['our_publisher_id'],
          device_type: generatedData['device_type'],
          format: generatedData['format'],
          tmax: generatedData['tmax'],
          geo_country: generatedData['geo_country'],
          crid: generatedData['crid'],
          repacked_crid: generatedData['repacked_crid'],
          broadcastRequests: totalRecords['ourBidRequests'],
          receivedResponses: totalRecords['bidResponses'],
          impressions: totalRecords['Impressions'],
          burls:0,
          nurls: totalRecords["recordsNurls"],
          ssp_spend: totalRecords["sspSpend"] * totalRecords["Impressions"],
          dsp_spend: totalRecords["dspSpend"] * totalRecords["Impressions"],
          demand_bidfloor: faker.number.float({ min: 0.01, max: 1.1 })
        });
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
      count: faker.number.int({ min: Math.ceil(statusCodesCount[statuscode] * 0.5), max: statusCodesCount[statuscode] })
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
