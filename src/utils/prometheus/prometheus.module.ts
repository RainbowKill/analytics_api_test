import { Module } from '@nestjs/common';
import { PrometheusModule } from '@willsoto/nestjs-prometheus';
import { PrometheusService } from './prometheus.service';

@Module({
  imports: [
    PrometheusModule.register({
      path: '/metrics', // Endpoint to expose metrics
      defaultMetrics: {
        enabled: true, // Enable default metrics (CPU, memory, etc.)
      },
    }),
  ],
  providers: [PrometheusService], // Register PrometheusService
  exports: [PrometheusService],
})
export class PrometheusPrometheusModule {}
