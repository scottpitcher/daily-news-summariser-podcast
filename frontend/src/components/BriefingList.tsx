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
    <ul className="bg-white divide-y divide-[#ece8dc] border border-[#e0dcd0] rounded-xl overflow-hidden">
      {briefings.map((briefing) => (
        <li
          key={`${briefing.date}-${briefing.topic}`}
          onClick={() => onSelect(briefing)}
          className="p-4 cursor-pointer hover:bg-[#a96b4d]/[0.06] flex items-center justify-between border-l-2 border-transparent hover:border-[#a96b4d] transition-colors duration-150"
        >
          <div>
            <p className="font-semibold text-[#211f1c]">{topicLabel(briefing.topic)}</p>
            <p className="text-sm text-gray-400 mt-0.5">{briefing.date}</p>
          </div>
          <span className="text-xs font-medium text-[#8c5736] bg-[#ecdcc9] rounded-full px-2.5 py-1">
            {briefing.articles.length} articles
          </span>
        </li>
      ))}
    </ul>
  );
}
