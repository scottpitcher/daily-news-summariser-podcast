import { TOPIC_LABELS } from "../topics";

interface FilterBarProps {
  topic: string;
  date: string;
  onTopicChange: (topic: string) => void;
  onDateChange: (date: string) => void;
}

function todayIso(): string {
  return new Date().toISOString().slice(0, 10);
}

export function FilterBar({ topic, date, onTopicChange, onDateChange }: FilterBarProps) {
  const isToday = date === todayIso();

  return (
    <div className="flex flex-wrap gap-4 items-end mb-6">
      <div>
        <label className="block text-sm font-medium text-gray-600 mb-1">Topic</label>
        <select
          value={topic}
          onChange={(e) => onTopicChange(e.target.value)}
          className="border border-gray-300 rounded-md px-3 py-2 text-sm"
        >
          <option value="">All topics</option>
          {Object.entries(TOPIC_LABELS).map(([key, label]) => (
            <option key={key} value={key}>
              {label}
            </option>
          ))}
        </select>
      </div>
      <div>
        <label className="block text-sm font-medium text-gray-600 mb-1">Date</label>
        <input
          type="date"
          value={date}
          onChange={(e) => onDateChange(e.target.value)}
          className="border border-gray-300 rounded-md px-3 py-2 text-sm"
        />
      </div>
      <button
        onClick={() => onDateChange(todayIso())}
        disabled={isToday}
        className="px-3 py-2 text-sm rounded-md bg-[#1a1a2e] text-white hover:bg-[#0f3460] disabled:opacity-50 disabled:cursor-not-allowed"
      >
        Today
      </button>
      {(topic || date) && (
        <button
          onClick={() => {
            onTopicChange("");
            onDateChange("");
          }}
          className="text-sm text-[#e94560] hover:underline pb-2"
        >
          Clear filters
        </button>
      )}
    </div>
  );
}
