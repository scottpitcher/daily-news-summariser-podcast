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
    <ul className="divide-y divide-gray-200 border border-gray-200 rounded-md">
      {briefings.map((briefing) => (
        <li
          key={`${briefing.date}-${briefing.topic}`}
          onClick={() => onSelect(briefing)}
          className="p-4 cursor-pointer hover:bg-gray-50 flex items-center justify-between border-l-4 border-transparent hover:border-[#0f3460]"
        >
          <div>
            <p className="font-medium text-[#1a1a2e]">{topicLabel(briefing.topic)}</p>
            <p className="text-sm text-gray-500">{briefing.date}</p>
          </div>
          <span className="text-sm text-gray-400">{briefing.articles.length} articles</span>
        </li>
      ))}
    </ul>
  );
}
