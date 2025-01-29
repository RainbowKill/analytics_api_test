import { Injectable, LoggerService } from '@nestjs/common';
import { Inject } from '@nestjs/common';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';
import { Logger } from 'winston';

@Injectable()
export class CustomLoggerService implements LoggerService {
  constructor(
    @Inject(WINSTON_MODULE_PROVIDER) private readonly logger: Logger,
  ) {}

  log(message: string) {
    this.logger.info({ message });
  }

  error(message: string, trace: string) {
    this.logger.error(
      JSON.stringify({
        message,
        trace: trace ? trace.replace(/\n/g, ' ') : undefined,
      }),
    );
  }

  warn(message: string) {
    this.logger.warn({ message });
  }

  debug(message: string) {
    this.logger.debug({ message });
  }

  verbose(message: string) {
    this.logger.verbose({ message });
  }
}
