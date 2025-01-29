import { Module } from '@nestjs/common';
import { WinstonModule } from 'nest-winston';
import * as winston from 'winston';
import { CustomLoggerService } from './logger.service';

@Module({
  imports: [
    WinstonModule.forRoot({
      transports: [
        new winston.transports.Console({
          format: winston.format.combine(
            winston.format.timestamp(),
            winston.format.printf(({ level, message, timestamp }) => {
              const sanitizedMessage = sanitizeMessage(message as string);
              return JSON.stringify({
                level,
                message: sanitizedMessage,
                timestamp,
              });
            }),
          ),
        }),
      ],
    }),
  ],
  providers: [CustomLoggerService],
  exports: [WinstonModule],
})
export class LoggerModule {}

function sanitizeMessage(message: string): string {
  if (typeof message !== 'string') {
    return JSON.stringify(message); // Ensure objects/arrays are stringified
  }

  // Escape quotes and backslashes to ensure proper JSON formatting
  return message.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
}
