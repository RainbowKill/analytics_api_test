import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { CustomLoggerService } from '@/utils/logger/logger.service';
const port = process.env.PORT || 3000;

// @TODO add logger port and host
// @TODO global filter
async function bootstrap() {
  const app = await NestFactory.create(AppModule, {
    bufferLogs: true,
  });

  app.useLogger(app.get(CustomLoggerService));
  await app.listen(port);
}
bootstrap();
