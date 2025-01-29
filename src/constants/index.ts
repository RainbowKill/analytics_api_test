import * as dotenv from 'dotenv';

dotenv.config();

export const startDay = process.env.START_ANALYTICS_TIME;
export const serviceName = process.env.SERVICE_NAME;
export const prodMode = process.env.USE_PROD_DB;
export const databaseName = process.env.CH_DB;
export const dictionarySuffix = '_' + (process.env.CH_DB || 'analytics');
