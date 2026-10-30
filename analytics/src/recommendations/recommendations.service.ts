import { Injectable } from '@nestjs/common';
import { ClickHouseService } from '../clickhouse/clickhouse.service';
import { UserScopeService } from '../common/user-scope.service';
import { COLD_START_SQL, PERSONAL_RECS_SQL } from './recommendations.sql';

interface PersonalRow {
  track_id: string;
  track_name: string;
  artist_name: string;
  genre: string;
  score: number;
  similarity: number;
  genre_match: number;
  danceability: number;
  energy: number;
  valence: number;
  acousticness: number;
  p_dance: number;
  p_energy: number;
  p_valence: number;
  p_acoustic: number;
  p_tempo: number;
  profile_tracks: number;
  top_genres: string[];
}

interface ColdRow {
  track_id: string;
  track_name: string;
  artist_name: string;
  genre: string;
  score: number;
  tenant_plays: number;
}

const r3 = (n: number) => Math.round(n * 1000) / 1000;

@Injectable()
export class RecommendationsService {
  constructor(
    private readonly ch: ClickHouseService,
    private readonly users: UserScopeService,
  ) {}

  async recommend(tenant: string, userId: string, limit: number) {
    await this.users.resolve(tenant, userId); // 404 outside tenant
    const params = { tenant, userId, limit };

    const rows = await this.ch.query<PersonalRow>(PERSONAL_RECS_SQL, params);
    if (rows.length) {
      const p = rows[0];
      return {
        user_id: userId,
        strategy: 'content_based',
        taste_profile: {
          danceability: r3(p.p_dance),
          energy: r3(p.p_energy),
          valence: r3(p.p_valence),
          acousticness: r3(p.p_acoustic),
          tempo_normalized: r3(p.p_tempo),
          top_genres: p.top_genres,
          based_on_tracks: p.profile_tracks,
          window_days: 90,
        },
        recommendations: rows.map((r) => ({
          track_id: r.track_id,
          track_name: r.track_name,
          artist_name: r.artist_name,
          genre: r.genre,
          score: r.score,
          reason: personalReason(r),
        })),
      };
    }

    const cold = await this.ch.query<ColdRow>(COLD_START_SQL, params);
    return {
      user_id: userId,
      strategy: 'cold_start',
      taste_profile: null,
      recommendations: cold.map((r) => ({
        track_id: r.track_id,
        track_name: r.track_name,
        artist_name: r.artist_name,
        genre: r.genre,
        score: r.score,
        reason: r.tenant_plays > 0 ? `popular with listeners on your team (${r.tenant_plays} plays)` : 'popular in the catalog',
      })),
    };
  }
}

/** Short human reason: genre match + the features where the track is strong and close to the user's taste. */
function personalReason(r: PersonalRow): string {
  const traits: string[] = [];
  const near = (a: number, b: number) => Math.abs(a - b) <= 0.2;
  if (r.energy >= 0.65 && near(r.energy, r.p_energy)) traits.push('high-energy');
  if (r.energy <= 0.35 && near(r.energy, r.p_energy)) traits.push('chill');
  if (r.danceability >= 0.65 && near(r.danceability, r.p_dance)) traits.push('danceable');
  if (r.valence >= 0.65 && near(r.valence, r.p_valence)) traits.push('upbeat');
  if (r.valence <= 0.35 && near(r.valence, r.p_valence)) traits.push('moody');
  if (r.acousticness >= 0.6 && near(r.acousticness, r.p_acoustic)) traits.push('acoustic');

  const parts = [r.genre_match ? r.genre : '', ...traits.slice(0, 2)].filter(Boolean);
  if (parts.length) return `matches your ${parts.join(' + ')} taste`;
  return `sounds similar to what you play (${Math.round(r.similarity * 100)}% audio match)`;
}
