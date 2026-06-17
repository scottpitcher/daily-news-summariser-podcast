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
          className="bg-white border border-[#e7e0c9] rounded-lg px-3.5 py-2.5 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-[#bc6c25]/20 focus:border-[#bc6c25] transition"
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
          className="text-sm font-medium text-[#bc6c25] hover:text-[#a85a1c] transition-colors pb-2.5"
        >
          Clear filter
        </button>
      )}
    </div>
  );
}
