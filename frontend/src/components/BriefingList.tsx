import type { Briefing } from "../api";
import { topicLabel } from "../topics";

interface BriefingListProps {
  briefings: Briefing[];
  onSelect: (briefing: Briefing) => void;
}

export function BriefingList({ briefings, onSelect }: BriefingListProps) {
  if (briefings.length === 0) {
    return <p className="text-gray-500">No briefings match the current filters.</p>;
  }

  return (
    <ul className="bg-white divide-y divide-gray-100 border border-gray-200 rounded-2xl shadow-sm overflow-hidden">
      {briefings.map((briefing) => (
        <li
          key={`${briefing.date}-${briefing.topic}`}
          onClick={() => onSelect(briefing)}
          className="p-4 cursor-pointer hover:bg-[#1a1a2e]/[0.03] flex items-center justify-between border-l-2 border-transparent hover:border-[#e94560] transition-colors duration-150"
        >
          <div>
            <p className="font-semibold text-[#1a1a2e]">{topicLabel(briefing.topic)}</p>
            <p className="text-sm text-gray-400 mt-0.5">{briefing.date}</p>
          </div>
          <span className="text-xs font-medium text-gray-600 bg-gray-200 rounded-full px-2.5 py-1">
            {briefing.articles.length} articles
          </span>
        </li>
      ))}
    </ul>
  );
}
