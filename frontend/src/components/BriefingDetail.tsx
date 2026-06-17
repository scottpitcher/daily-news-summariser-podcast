import type { Briefing } from "../api";
import { resolveAudioUrl } from "../api";
import { topicLabel } from "../topics";

interface BriefingDetailProps {
  briefing: Briefing;
  onBack: () => void;
}

export function BriefingDetail({ briefing, onBack }: BriefingDetailProps) {
  const audioUrl = resolveAudioUrl(briefing.audioUrl);

  return (
    <div>
      <button onClick={onBack} className="text-sm text-[#1a1a2e] hover:underline mb-4">
        ← Back to briefings
      </button>

      <p className="text-sm text-gray-500 mb-4">{briefing.date}</p>

      {audioUrl ? (
        <audio controls src={audioUrl} className="w-full mb-6" />
      ) : (
        <p className="text-sm text-gray-400 mb-6">No audio available for this briefing.</p>
      )}

      <h2 className="text-[#1a1a2e] text-lg font-semibold pb-2 mb-5 border-b-2 border-[#0f3460]">
        {topicLabel(briefing.topic)}
      </h2>

      <div className="space-y-6">
        {briefing.articles.map((article) => (
          <div key={`${article.rank}-${article.title}`}>
            <h3 className="text-[#1a1a2e] text-base font-bold mb-1">{article.headline}</h3>
            {article.bullets.length > 0 && (
              <ul className="list-disc pl-5 space-y-1 mb-2">
                {article.bullets.map((bullet, i) => (
                  <li key={i} className="text-sm text-gray-700 leading-relaxed">
                    {bullet}
                  </li>
                ))}
              </ul>
            )}
            {article.soWhat && (
              <p className="text-sm text-gray-700 leading-relaxed mb-2">
                <span className="font-bold text-[#e94560]">So what? </span>
                {article.soWhat}
              </p>
            )}
            <p className="text-xs text-gray-500">
              Source:{" "}
              {article.sourceUrl ? (
                <a
                  href={article.sourceUrl}
                  target="_blank"
                  rel="noreferrer"
                  className="underline hover:text-[#1a1a2e]"
                >
                  {article.title || "Source"}
                </a>
              ) : (
                article.title || "Source unavailable"
              )}{" "}
              — {article.source}
            </p>
          </div>
        ))}
      </div>
    </div>
  );
}
