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
    <div className="bg-white rounded-xl border border-[#e5e3da] p-6">
      <button
        onClick={onBack}
        className="text-sm font-medium text-gray-500 hover:text-[#141413] transition-colors mb-4"
      >
        ← Back to briefings
      </button>

      <p className="text-sm text-gray-400 mb-4">{briefing.date}</p>

      {audioUrl ? (
        <audio controls src={audioUrl} className="w-full mb-6 rounded-lg" />
      ) : (
        <p className="text-sm text-gray-400 mb-6">No audio available for this briefing.</p>
      )}

      <h2 className="text-[#141413] text-xl font-bold pb-3 mb-5 border-b border-[#e5e3da]">
        {topicLabel(briefing.topic)}
      </h2>

      <div className="space-y-7">
        {briefing.articles.map((article) => (
          <div key={`${article.rank}-${article.title}`}>
            <h3 className="text-[#141413] text-base font-bold mb-1.5">{article.headline}</h3>
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
              <p className="text-sm text-gray-700 leading-relaxed mb-2.5 bg-[#cc785c]/[0.08] rounded-lg px-3 py-2">
                <span className="font-bold text-[#a8512f]">So what? </span>
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
                  className="underline hover:text-[#141413] transition-colors"
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
