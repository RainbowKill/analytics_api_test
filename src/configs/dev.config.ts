import * as dotenv from 'dotenv';

dotenv.config();

export const AnalyticsClickhouseConfig = () => ({
  host: process.env.CH_HOST || '',
  username: process.env.CH_USER || '',
  password: process.env.CH_PASSWORD || '',
  database: process.env.CH_DB || '',
});

export const dbName = process.env.CH_DB || '';
