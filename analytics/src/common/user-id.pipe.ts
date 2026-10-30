import { BadRequestException, Injectable, PipeTransform } from '@nestjs/common';

const USER_ID_RE = /^[A-Za-z0-9_.:@-]{1,128}$/;

@Injectable()
export class UserIdPipe implements PipeTransform<string, string> {
  transform(value: string): string {
    if (!USER_ID_RE.test(value)) throw new BadRequestException('Invalid userId');
    return value;
  }
}
