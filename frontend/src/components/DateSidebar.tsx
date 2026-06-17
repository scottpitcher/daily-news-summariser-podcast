import { useMemo } from "react";

interface DateSidebarProps {
  dates: string[];
  selectedDate: string;
  onSelectDate: (date: string) => void;
}

function formatReportLabel(date: string): string {
  const parsed = new Date(`${date}T00:00:00`);
  return `Report for ${parsed.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  })}`;
}

function monthYearKey(date: string): string {
  const parsed = new Date(`${date}T00:00:00`);
  return parsed.toLocaleDateString("en-US", { month: "long", year: "numeric" });
}

export function DateSidebar({ dates, selectedDate, onSelectDate }: DateSidebarProps) {
  const groups = useMemo(() => {
    const ordered: { label: string; dates: string[] }[] = [];
    for (const date of dates) {
      const label = monthYearKey(date);
      const lastGroup = ordered[ordered.length - 1];
      if (lastGroup && lastGroup.label === label) {
        lastGroup.dates.push(date);
      } else {
        ordered.push({ label, dates: [date] });
      }
    }
    return ordered;
  }, [dates]);

  return (
    <aside className="h-full w-64 shrink-0 bg-white overflow-y-auto">
      {groups.map((group) => (
        <div key={group.label}>
          <div className="sticky top-0 bg-[#f1f1f6] border-y border-[#e4e4ea] text-[11px] font-semibold text-[#6b6b80] uppercase tracking-wider px-4 py-2">
            {group.label}
          </div>
          <ul>
            {group.dates.map((date) => (
              <li key={date}>
                <button
                  onClick={() => onSelectDate(date)}
                  className={`w-full text-left px-4 py-2.5 text-sm border-l-2 transition-colors duration-150 ${
                    date === selectedDate
                      ? "border-[#6366f1] bg-[#6366f1]/[0.07] text-[#18181b] font-semibold"
                      : "border-transparent hover:bg-[#f7f7fb] text-gray-600 hover:text-[#18181b]"
                  }`}
                >
                  {formatReportLabel(date)}
                </button>
              </li>
            ))}
          </ul>
        </div>
      ))}
      {dates.length === 0 && <p className="text-sm text-gray-400 px-4 py-3">No reports available.</p>}
    </aside>
  );
}
