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
    <ul className="bg-white divide-y divide-[#f0eee5] border border-[#e8e5dc] rounded-xl overflow-hidden">
      {briefings.map((briefing) => (
        <li
          key={`${briefing.date}-${briefing.topic}`}
          onClick={() => onSelect(briefing)}
          className="p-4 cursor-pointer hover:bg-[#bd8369]/[0.05] flex items-center justify-between border-l-2 border-transparent hover:border-[#bd8369] transition-colors duration-150"
        >
          <div>
            <p className="font-semibold text-[#2b2a28]">{topicLabel(briefing.topic)}</p>
            <p className="text-sm text-gray-400 mt-0.5">{briefing.date}</p>
          </div>
          <span className="text-xs font-medium text-[#a8704f] bg-[#f2e7dd] rounded-full px-2.5 py-1">
            {briefing.articles.length} articles
          </span>
        </li>
      ))}
    </ul>
  );
}
