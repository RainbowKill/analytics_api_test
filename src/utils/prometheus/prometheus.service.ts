import { Injectable, Inject } from '@nestjs/common';
import { Counter, Histogram, register } from 'prom-client';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';
import { Logger } from 'winston';
import { serviceName } from '@/constants';

const customMetrics = {};

@Injectable()
export class PrometheusService {
  constructor(
    @Inject(WINSTON_MODULE_PROVIDER) private readonly logger: Logger,
  ) {}

  public async inc(key) {
    let calculatedKey = `${serviceName}_${key}`;
    calculatedKey = calculatedKey.replace(/-/g, '_');
    try {
      if (!customMetrics[calculatedKey]) {
        customMetrics[calculatedKey] = new Counter({
          name: calculatedKey,
          help: calculatedKey,
        });
      }
    } catch (e) {
      // await this.gsLogger.log(WinstonLogLevel.debug, new LogPayload('GSPrometheusService', 'inc', `error on creatin prometheus key: ${calculatedKey}`))
      this.logger.error(`error on creatin prometheus key: ${calculatedKey}`);
    }

    customMetrics[calculatedKey].inc();
  }

  // timer
  public async getTimer(key) {
    let calculatedKey = `test_${key}`;
    calculatedKey = calculatedKey.replace(/-/g, '_');
    if (!customMetrics[calculatedKey]) {
      customMetrics[calculatedKey] = new Histogram({
        name: calculatedKey,
        help: 'Duration of HTTP requests in microseconds',
        buckets: [0.1, 0.5, 1, 1.5],
      });
    }

    return customMetrics[calculatedKey];
  }

  getMetrics(): unknown {
    this.logger.log({
      message: 'getMetrics',
      level: 'info',
    });
    return register.metrics();
  }
}
