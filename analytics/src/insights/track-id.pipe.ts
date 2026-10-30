import { BadRequestException, Injectable, PipeTransform } from '@nestjs/common';

// Catalog ids look like `spotify:track:a1G3BmW1AqUoJR23YaFeCI`.
const TRACK_ID_RE = /^[A-Za-z0-9_.:-]{1,128}$/;

@Injectable()
export class TrackIdPipe implements PipeTransform<string, string> {
  transform(value: string): string {
    if (!TRACK_ID_RE.test(value)) throw new BadRequestException('Invalid trackId');
    return value;
  }
}
