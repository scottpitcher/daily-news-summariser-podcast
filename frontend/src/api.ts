export interface Article {
  title: string;
  headline: string;
  bullets: string[];
  soWhat: string;
  source: string;
  sourceUrl: string | null;
  rank: number;
  tags: string[];
}

export interface Briefing {
  date: string;
  topic: string;
  audioUrl: string | null;
  articles: Article[];
}

const GRAPHQL_ENDPOINT = "http://localhost:8000/graphql";
const API_BASE = "http://localhost:8000";

const BRIEFINGS_QUERY = `
  query Briefings($topic: String, $date: String) {
    briefings(topic: $topic, date: $date) {
      date
      topic
      audioUrl
      articles {
        title
        headline
        bullets
        soWhat
        source
        sourceUrl
        rank
        tags
      }
    }
  }
`;

export async function fetchBriefings(filters: { topic?: string; date?: string }): Promise<Briefing[]> {
  const response = await fetch(GRAPHQL_ENDPOINT, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      query: BRIEFINGS_QUERY,
      variables: { topic: filters.topic || null, date: filters.date || null },
    }),
  });

  const { data, errors } = await response.json();
  if (errors?.length) {
    throw new Error(errors[0].message);
  }
  return data.briefings;
}

export function resolveAudioUrl(audioUrl: string | null): string | null {
  if (!audioUrl) return null;
  return `${API_BASE}${audioUrl}`;
}
