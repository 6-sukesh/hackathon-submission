import { Module } from '@nestjs/common';
import { InsightsService } from './insights.service';
import { TrackInsightsController, UserInsightsController } from './insights.controller';

@Module({ controllers: [UserInsightsController, TrackInsightsController], providers: [InsightsService] })
export class InsightsModule {}
