import { Global, Module } from '@nestjs/common';
import { UserScopeService } from './user-scope.service';

@Global()
@Module({ providers: [UserScopeService], exports: [UserScopeService] })
export class CommonModule {}
