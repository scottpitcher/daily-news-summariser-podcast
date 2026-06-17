import { TOPIC_LABELS } from "../topics";

interface FilterBarProps {
  topic: string;
  onTopicChange: (topic: string) => void;
}

export function FilterBar({ topic, onTopicChange }: FilterBarProps) {
  return (
    <div className="flex flex-wrap gap-4 items-end mb-6">
      <div>
        <label className="block text-sm font-medium text-gray-600 mb-1">Topic</label>
        <select
          value={topic}
          onChange={(e) => onTopicChange(e.target.value)}
          className="bg-white border border-gray-300 rounded-md px-3 py-2 text-sm"
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
        <button onClick={() => onTopicChange("")} className="text-sm text-[#e94560] hover:underline pb-2">
          Clear filter
        </button>
      )}
    </div>
  );
}
