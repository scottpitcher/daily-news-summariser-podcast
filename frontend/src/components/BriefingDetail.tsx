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
    <div className="bg-white rounded-2xl border border-[#e4e4ea] shadow-sm p-6">
      <button
        onClick={onBack}
        className="text-sm font-medium text-gray-500 hover:text-[#18181b] transition-colors mb-4"
      >
        ← Back to briefings
      </button>

      <p className="text-sm text-gray-400 mb-4">{briefing.date}</p>

      {audioUrl ? (
        <audio controls src={audioUrl} className="w-full mb-6 rounded-lg" />
      ) : (
        <p className="text-sm text-gray-400 mb-6">No audio available for this briefing.</p>
      )}

      <h2 className="text-[#18181b] text-xl font-bold pb-3 mb-5 border-b border-[#e4e4ea]">
        {topicLabel(briefing.topic)}
      </h2>

      <div className="space-y-7">
        {briefing.articles.map((article) => (
          <div key={`${article.rank}-${article.title}`}>
            <h3 className="text-[#18181b] text-base font-bold mb-1.5">{article.headline}</h3>
            {article.bullets.length > 0 && (
              <ul className="list-disc pl-5 space-y-1 mb-2.5">
                {article.bullets.map((bullet, i) => (
                  <li key={i} className="text-sm text-gray-600 leading-relaxed">
                    {bullet}
                  </li>
                ))}
              </ul>
            )}
            {article.soWhat && (
              <p className="text-sm text-gray-700 leading-relaxed mb-2.5 bg-[#6366f1]/[0.06] rounded-lg px-3 py-2">
                <span className="font-bold text-[#4338ca]">So what? </span>
                {article.soWhat}
              </p>
            )}
            <p className="text-xs text-gray-400">
              Source:{" "}
              {article.sourceUrl ? (
                <a
                  href={article.sourceUrl}
                  target="_blank"
                  rel="noreferrer"
                  className="underline hover:text-[#18181b] transition-colors"
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
