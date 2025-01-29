import { RootTables } from '@/enums/tables';
import { databaseName } from '@/constants';

export const generateHourlyAggregatedOurRequests = ({
  table,
  lastProcessedTime,
  lastProcessedTimePlusPeriod,
}: {
  table: string;
  lastProcessedTime: string;
  lastProcessedTimePlusPeriod: string;
}): string => {
  return `
        INSERT INTO ${databaseName}.${table}
        WITH
            our_filtered_requests AS (
                SELECT
                    toStartOfHour(event_time) AS time,
                    ssp_endpoint_id,
                    dsp_endpoint_id,
                    bundle_domain,
                    size,
                    publisher_id,
                    our_publisher_id,
                    device_type,
                    format,
                    tmax,
                    geo_country,
                    sum(count) AS our_requests,
                    AVG(demand_bidfloor) AS demand_bidfloor
                FROM
                    ${databaseName}.${RootTables.OurBidRequests}
                WHERE
                    event_time >= '${lastProcessedTime}'
                    AND event_time < '${lastProcessedTimePlusPeriod}'
                GROUP BY
                    time, ssp_endpoint_id, dsp_endpoint_id, bundle_domain, size, publisher_id, our_publisher_id, device_type, format, tmax, geo_country
            ),
            filtered_responses AS (
                SELECT
                    toStartOfHour(event_time) AS time,
                    ssp_endpoint_id,
                    dsp_endpoint_id,
                    bundle_domain,
                    size,
                    publisher_id,
                    our_publisher_id,
                    device_type,
                    format,
                    tmax,
                    geo_country,
                    sum(count) AS responses
                FROM
                    ${databaseName}.${RootTables.BidResponses}
                WHERE
                    event_time >= '${lastProcessedTime}'
                    AND event_time < '${lastProcessedTimePlusPeriod}'
                GROUP BY
                    time, ssp_endpoint_id, dsp_endpoint_id, bundle_domain, size, publisher_id, our_publisher_id, device_type, format, tmax, geo_country
            ),
            filtered_impressions AS (
                SELECT
                    toStartOfHour(event_time) AS time,
                    ssp_endpoint_id,
                    dsp_endpoint_id,
                    bundle_domain,
                    size,
                    publisher_id,
                    our_publisher_id,
                    device_type,
                    format,
                    tmax,
                    geo_country,
                    sum(count) AS impressions,
                    sum(ssp_spend) as ssp_revenue,
                    sum(dsp_spend) as dsp_revenue
                FROM
                    ${databaseName}.${RootTables.Impressions}
                WHERE
                    event_time >= '${lastProcessedTime}'
                    AND event_time < '${lastProcessedTimePlusPeriod}'
                GROUP BY
                    time, ssp_endpoint_id, dsp_endpoint_id, bundle_domain, size, publisher_id, our_publisher_id, device_type, format, tmax, geo_country
            ),
            filtered_burls AS (
                SELECT
                    toStartOfHour(event_time) AS time,
                    ssp_endpoint_id,
                    dsp_endpoint_id,
                    bundle_domain,
                    size,
                    publisher_id,
                    our_publisher_id,
                    device_type,
                    format,
                    tmax,
                    geo_country,
                    sum(count) AS burls
                FROM
                    ${databaseName}.${RootTables.Burls}
                WHERE
                    event_time >= '${lastProcessedTime}'
                    AND event_time < '${lastProcessedTimePlusPeriod}'
                GROUP BY
                    time, ssp_endpoint_id, dsp_endpoint_id, bundle_domain, size, publisher_id, our_publisher_id, device_type, format, tmax, geo_country
            ),
            filtered_nurls AS (
                SELECT
                    toStartOfHour(event_time) AS time,
                    ssp_endpoint_id,
                    dsp_endpoint_id,
                    bundle_domain,
                    size,
                    publisher_id,
                    our_publisher_id,
                    device_type,
                    format,
                    tmax,
                    geo_country,
                    sum(count) AS nurls
                FROM
                    ${databaseName}.${RootTables.Nurls}
                WHERE
                    event_time >= '${lastProcessedTime}'
                    AND event_time < '${lastProcessedTimePlusPeriod}'
                GROUP BY
                    time, ssp_endpoint_id, dsp_endpoint_id, bundle_domain, size, publisher_id, our_publisher_id, device_type, format, tmax, geo_country
            )
        SELECT
            IF(bidReq.time > '2020-01-01', bidReq.time, 
                IF(res.time > '2020-01-01', res.time, 
                IF(imp.time > '2020-01-01', imp.time, 
                IF(burl.time > '2020-01-01', burl.time, 
                nurl.time)))) AS time,
            
            IF(bidReq.ssp_endpoint_id > 0, bidReq.ssp_endpoint_id, 
                IF(res.ssp_endpoint_id > 0, res.ssp_endpoint_id, 
                IF(imp.ssp_endpoint_id > 0, imp.ssp_endpoint_id, 
                IF(burl.ssp_endpoint_id > 0, burl.ssp_endpoint_id, 
                nurl.ssp_endpoint_id)))) AS ssp_endpoint_id,
            
            IF(bidReq.dsp_endpoint_id > 0, bidReq.dsp_endpoint_id, 
                IF(res.dsp_endpoint_id > 0, res.dsp_endpoint_id, 
                IF(imp.dsp_endpoint_id > 0, imp.dsp_endpoint_id, 
                IF(burl.dsp_endpoint_id > 0, burl.dsp_endpoint_id, 
                nurl.dsp_endpoint_id)))) AS dsp_endpoint_id,
            
            IF(LENGTH(bidReq.bundle_domain) > 0, bidReq.bundle_domain, 
                IF(LENGTH(res.bundle_domain) > 0, res.bundle_domain, 
                IF(LENGTH(imp.bundle_domain) > 0, imp.bundle_domain, 
                IF(LENGTH(burl.bundle_domain) > 0, burl.bundle_domain, 
                nurl.bundle_domain)))) AS bundle_domain,
                
            IF(LENGTH(bidReq.size) > 0, bidReq.size, 
                IF(LENGTH(res.size) > 0, res.size, 
                IF(LENGTH(imp.size) > 0, imp.size, 
                IF(LENGTH(burl.size) > 0, burl.size, 
                nurl.size)))) AS size,
            
            IF(LENGTH(bidReq.publisher_id) > 0, bidReq.publisher_id, 
                IF(LENGTH(res.publisher_id) > 0, res.publisher_id, 
                IF(LENGTH(imp.publisher_id) > 0, imp.publisher_id, 
                IF(LENGTH(burl.publisher_id) > 0, burl.publisher_id, 
                nurl.publisher_id)))) AS publisher_id,
            
            IF(LENGTH(bidReq.our_publisher_id) > 0, bidReq.our_publisher_id, 
                IF(LENGTH(res.our_publisher_id) > 0, res.our_publisher_id, 
                IF(LENGTH(imp.our_publisher_id) > 0, imp.our_publisher_id, 
                IF(LENGTH(burl.our_publisher_id) > 0, burl.our_publisher_id, 
                nurl.our_publisher_id)))) AS our_publisher_id,
            
            IF(bidReq.device_type > 0, bidReq.device_type, 
                IF(res.device_type > 0, res.device_type, 
                IF(imp.device_type > 0, imp.device_type, 
                IF(burl.device_type > 0, burl.device_type, 
                nurl.device_type)))) AS device_type,
            
            IF(LENGTH(bidReq.format) > 0, bidReq.format, 
                IF(LENGTH(res.format) > 0, res.format, 
                IF(LENGTH(imp.format) > 0, imp.format, 
                IF(LENGTH(burl.format) > 0, burl.format, 
                nurl.format)))) AS format,
            
            IF(bidReq.tmax > 0, bidReq.tmax, 
                IF(res.tmax > 0, res.tmax, 
                IF(imp.tmax > 0, imp.tmax, 
                IF(burl.tmax > 0, burl.tmax, 
                nurl.tmax)))) AS tmax,

            IF(LENGTH(bidReq.geo_country) > 0, bidReq.geo_country, 
               ' IF(LENGTH(res.geo_country) > 0, res.geo_country, 
                IF(LENGTH(imp.geo_country) > 0, imp.geo_country, 
                IF(LENGTH(burl.geo_country) > 0, burl.geo_country, 
                nurl.geo_country)))') AS geo_country,
            
            sum(IFNULL(bidReq.our_requests, 0)) AS broadcastRequests,
            sum(IFNULL(res.responses, 0)) AS receivedResponses,
            sum(IFNULL(imp.impressions, 0)) AS impressions,
            sum(IFNULL(burl.burls, 0)) AS burls,
            sum(IFNULL(nurl.nurls, 0)) AS nurls,
            sum(IFNULL(imp.ssp_revenue, 0)) AS ssp_revenue,
            sum(IFNULL(imp.dsp_revenue, 0)) AS dsp_revenue,
            AVG(bidReq.demand_bidfloor) AS demand_bidfloor
        FROM
            our_filtered_requests AS bidReq
        FULL JOIN
            filtered_responses AS res
            ON bidReq.time = res.time
            AND bidReq.ssp_endpoint_id = res.ssp_endpoint_id
            AND bidReq.dsp_endpoint_id = res.dsp_endpoint_id
            AND bidReq.bundle_domain = res.bundle_domain
            AND bidReq.size = res.size
            AND bidReq.publisher_id = res.publisher_id
            AND bidReq.our_publisher_id = res.our_publisher_id
            AND bidReq.device_type = res.device_type
            AND bidReq.format = res.format
            AND bidReq.tmax = res.tmax
            AND bidReq.geo_country = res.geo_country
        FULL JOIN
            filtered_impressions AS imp
            ON bidReq.time = imp.time
            AND bidReq.ssp_endpoint_id = imp.ssp_endpoint_id
            AND bidReq.dsp_endpoint_id = imp.dsp_endpoint_id
            AND bidReq.bundle_domain = imp.bundle_domain
            AND bidReq.size = imp.size
            AND bidReq.publisher_id = imp.publisher_id
            AND bidReq.our_publisher_id = imp.our_publisher_id
            AND bidReq.device_type = imp.device_type
            AND bidReq.format = imp.format
            AND bidReq.tmax = imp.tmax
            AND bidReq.geo_country = imp.geo_country
        FULL JOIN
            filtered_burls AS burl
            ON bidReq.time = burl.time
            AND bidReq.ssp_endpoint_id = burl.ssp_endpoint_id
            AND bidReq.dsp_endpoint_id = burl.dsp_endpoint_id
            AND bidReq.bundle_domain = burl.bundle_domain
            AND bidReq.size = burl.size
            AND bidReq.publisher_id = burl.publisher_id
            AND bidReq.our_publisher_id = burl.our_publisher_id
            AND bidReq.device_type = burl.device_type
            AND bidReq.format = burl.format
            AND bidReq.tmax = burl.tmax
            AND bidReq.geo_country = burl.geo_country
        FULL JOIN
            filtered_nurls AS nurl
            ON bidReq.time = nurl.time
            AND bidReq.ssp_endpoint_id = nurl.ssp_endpoint_id
            AND bidReq.dsp_endpoint_id = nurl.dsp_endpoint_id
            AND bidReq.bundle_domain = nurl.bundle_domain
            AND bidReq.size = nurl.size
            AND bidReq.publisher_id = nurl.publisher_id
            AND bidReq.our_publisher_id = nurl.our_publisher_id
            AND bidReq.device_type = nurl.device_type
            AND bidReq.format = nurl.format
            AND bidReq.tmax = nurl.tmax
            AND bidReq.geo_country = nurl.geo_country
        GROUP BY
            time,
            ssp_endpoint_id,
            dsp_endpoint_id,
            bundle_domain,
            size,
            publisher_id,
            our_publisher_id,
            device_type,
            format,
            tmax,
            geo_country

        ORDER BY time, ssp_endpoint_id ASC;
      `;
};
