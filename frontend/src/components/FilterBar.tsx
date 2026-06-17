import { TOPIC_LABELS } from "../topics";

interface FilterBarProps {
  topic: string;
  onTopicChange: (topic: string) => void;
}

export function FilterBar({ topic, onTopicChange }: FilterBarProps) {
  return (
    <div className="flex flex-wrap gap-4 items-end mb-6">
      <div>
        <label className="block text-xs font-semibold text-gray-500 uppercase tracking-wide mb-1.5">Topic</label>
        <select
          value={topic}
          onChange={(e) => onTopicChange(e.target.value)}
          className="bg-white border border-gray-200 rounded-lg px-3.5 py-2.5 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-[#0f3460]/20 focus:border-[#0f3460] transition"
        >
          <option value="">All topics</option>
          {Object.entries(TOPIC_LABELS).map(([key, label]) => (
            <option key={key} value={key}>
              {label}
            </option>
          ))}
        </select>
      </div>
      {topic && (
        <button
          onClick={() => onTopicChange("")}
          className="text-sm font-medium text-[#e94560] hover:text-[#c5374f] transition-colors pb-2.5"
        >
          Clear filter
        </button>
      )}
    </div>
  );
}
