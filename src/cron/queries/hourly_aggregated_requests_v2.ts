export const generateHourlyAggregatedRequestsV2 = ({
  table,
  lastProcessedTime,
}: {
  table: string;
  lastProcessedTime: string;
}): string => {
  return `
        INSERT INTO analytics.${table}
        WITH
            filtered_requests AS (
                SELECT
                    toStartOfHour(event_time) AS time,
                    ssp_endpoint_id,
                    bundle_domain,
                    sum(count) AS sps_requests
                FROM
                    analytics.v2_BidRequests_Merge
                WHERE
                    event_time > '${lastProcessedTime}'  -- Start from last processed time
                    AND event_time < toStartOfDay(now())  -- End before the current day starts
                GROUP BY
                    time, ssp_endpoint_id, bundle_domain
            ),
            our_filtered_requests AS (
                SELECT
                    toStartOfHour(event_time) AS time,
                    ssp_endpoint_id,
                    bundle_domain,
                    sum(count) AS our_requests
                FROM
                    analytics.v2_OurBidRequests_Merge
                WHERE
                    event_time > '${lastProcessedTime}'
                    AND event_time < toStartOfDay(now())
                GROUP BY
                    time, ssp_endpoint_id, bundle_domain
            ),
            filtered_responses AS (
                SELECT
                    toStartOfHour(event_time) AS time,
                    ssp_endpoint_id,
                    bundle_domain,
                    sum(count) AS responses
                FROM
                    analytics.v2_BidResponses_Merge
                WHERE
                    event_time > '${lastProcessedTime}'
                    AND event_time < toStartOfDay(now())
                GROUP BY
                    time, ssp_endpoint_id, bundle_domain
            ),
            filtered_impressions AS (
                SELECT
                    toStartOfHour(event_time) AS time,
                    ssp_endpoint_id,
                    bundle_domain,
                    sum(count) AS impressions,
                    sum(ssp_spend) as ssp_revenue,
                    sum(dsp_spend) as dsp_revenue
                FROM
                    analytics.v2_Impressions_Merge
                WHERE
                    event_time > '${lastProcessedTime}'
                    AND event_time < toStartOfDay(now())
                GROUP BY
                    time, ssp_endpoint_id, bundle_domain
            ),
            filtered_burls AS (
                SELECT
                    toStartOfHour(event_time) AS time,
                    ssp_endpoint_id,
                    bundle_domain,
                    sum(count) AS burls
                FROM
                    analytics.v2_Burls_Merge
                WHERE
                    event_time > '${lastProcessedTime}'
                    AND event_time < toStartOfDay(now())
                GROUP BY
                    time, ssp_endpoint_id, bundle_domain
            ),
            filtered_nurls AS (
                SELECT
                    toStartOfHour(event_time) AS time,
                    ssp_endpoint_id,
                    bundle_domain,
                    sum(count) AS nurls
                FROM
                    analytics.v2_Nurls_Merge
                WHERE
                    event_time > '${lastProcessedTime}'
                    AND event_time < toStartOfDay(now())
                GROUP BY
                    time, ssp_endpoint_id, bundle_domain
            )
        SELECT
            req.time AS time,
            req.ssp_endpoint_id as ssp_endpoint_id,
            req.bundle_domain as bundle_domain,
            sum(req.sps_requests) AS incomingRequests,
            sum(bidReq.our_requests) AS broadcastRequests,
            sum(res.responses) AS receivedResponses,
            sum(imp.impressions) AS impressions,
            sum(burl.burls) AS burls,
            sum(nurl.nurls) AS nurls,
            sum(imp.ssp_revenue) as ssp_revenue,
            sum(imp.dsp_revenue) as dsp_revenue
        FROM
            filtered_requests AS req
        LEFT JOIN
            our_filtered_requests AS bidReq
            ON req.time = bidReq.time
            AND req.ssp_endpoint_id = bidReq.ssp_endpoint_id
            AND req.bundle_domain = bidReq.bundle_domain
        LEFT JOIN
            filtered_responses AS res
            ON req.time = res.time
            AND req.ssp_endpoint_id = res.ssp_endpoint_id
            AND req.bundle_domain = res.bundle_domain
        LEFT JOIN
            filtered_impressions AS imp
            ON req.time = imp.time
            AND req.ssp_endpoint_id = imp.ssp_endpoint_id
            AND req.bundle_domain = imp.bundle_domain
        LEFT JOIN
            filtered_burls AS burl
            ON req.time = burl.time
            AND req.ssp_endpoint_id = burl.ssp_endpoint_id
            AND req.bundle_domain = burl.bundle_domain
        LEFT JOIN
            filtered_nurls AS nurl
            ON req.time = nurl.time
            AND req.ssp_endpoint_id = nurl.ssp_endpoint_id
            AND req.bundle_domain = nurl.bundle_domain
        GROUP BY
            req.time,
            req.ssp_endpoint_id,
            req.bundle_domain
        ORDER BY time, ssp_endpoint_id ASC;
      `;
};
