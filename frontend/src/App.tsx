import { useEffect, useState } from "react";
import { fetchAvailableDates, fetchBriefings, type Briefing } from "./api";
import { Header } from "./components/Header";
import { BriefingList } from "./components/BriefingList";
import { BriefingDetail } from "./components/BriefingDetail";
import { DateSidebar } from "./components/DateSidebar";

function App() {
  const [date, setDate] = useState("");
  const [availableDates, setAvailableDates] = useState<string[]>([]);
  const [briefings, setBriefings] = useState<Briefing[]>([]);
  const [selected, setSelected] = useState<Briefing | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [sidebarOpen, setSidebarOpen] = useState(false);

  useEffect(() => {
    fetchAvailableDates()
      .then((dates) => {
        setAvailableDates(dates);
        setDate((current) => current || dates[0] || "");
      })
      .catch((err) => setError(err.message));
  }, []);

  useEffect(() => {
    if (!date) return;
    setLoading(true);
    setError(null);
    fetchBriefings({ date })
      .then(setBriefings)
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [date]);

  const selectDate = (newDate: string) => {
    setSelected(null);
    setDate(newDate);
  };

  return (
    <div className="max-w-3xl mx-auto px-4 py-8 relative">
      <div className="hidden xl:block absolute top-8 bottom-8 right-full mr-6 w-64 rounded-xl border border-[#e8e5dc] overflow-hidden">
        <DateSidebar dates={availableDates} selectedDate={date} onSelectDate={selectDate} />
      </div>

      <button
        onClick={() => setSidebarOpen(true)}
        className="xl:hidden mb-4 inline-flex items-center gap-2 bg-white border border-[#e8e5dc] rounded-lg px-3.5 py-2 text-sm font-medium text-[#2b2a28] hover:bg-[#f0eee5] transition-colors"
      >
        <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5">
          <path d="M1 4h14M1 8h14M1 12h14" strokeLinecap="round" />
        </svg>
        Choose date
      </button>

      {sidebarOpen && (
        <div className="xl:hidden fixed inset-0 z-50 flex">
          <div className="absolute inset-0 bg-black/40" onClick={() => setSidebarOpen(false)} />
          <div className="relative w-72 h-full bg-white shadow-xl overflow-y-auto">
            <DateSidebar
              dates={availableDates}
              selectedDate={date}
              onSelectDate={(newDate) => {
                selectDate(newDate);
                setSidebarOpen(false);
              }}
            />
          </div>
        </div>
      )}

      <Header />

      {selected ? (
        <BriefingDetail briefing={selected} onBack={() => setSelected(null)} />
      ) : (
        <>
          {loading && <p className="text-gray-500">Loading briefings…</p>}
          {error && <p className="text-red-600">Failed to load briefings: {error}</p>}
          {!loading && !error && <BriefingList briefings={briefings} onSelect={setSelected} />}
        </>
      )}
    </div>
  );
}

export default App;
