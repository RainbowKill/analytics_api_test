import { prodMode } from '@/constants';

// @TODO move to env variables
const prefix = prodMode === 'true' ? '' : 'test_'; // for testing

export const aggregatedTables = {
  Requests: `${prefix}hourly_aggregated_requests_v3`,
  OurRequests: `${prefix}hourly_aggregated_our_requests_v3`,
  Impressions: `${prefix}hourly_aggregated_impressions_v3`,
};

export const dailyUpdateState = prefix + 'daily_update_state';

export enum RootTables {
  BidRequests = 'v5_BidRequests_Merge',
  OurBidRequests = 'v5_OurBidRequests_Merge',
  BidResponses = 'v5_BidResponses_Merge',
  Impressions = 'v5_Impressions_Merge',
  Burls = 'v5_Burls_Merge',
  Nurls = 'v5_Nurls_Merge',
}
