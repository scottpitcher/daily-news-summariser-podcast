import { useEffect, useState } from "react";
import { fetchAvailableDates, fetchBriefings, type Briefing } from "./api";
import { Header } from "./components/Header";
import { FilterBar } from "./components/FilterBar";
import { BriefingList } from "./components/BriefingList";
import { BriefingDetail } from "./components/BriefingDetail";
import { DateSidebar } from "./components/DateSidebar";

function App() {
  const [topic, setTopic] = useState("");
  const [date, setDate] = useState("");
  const [availableDates, setAvailableDates] = useState<string[]>([]);
  const [briefings, setBriefings] = useState<Briefing[]>([]);
  const [selected, setSelected] = useState<Briefing | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

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
    fetchBriefings({ topic, date })
      .then(setBriefings)
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [topic, date]);

  return (
    <div className="max-w-3xl mx-auto px-4 py-8 relative">
      <div className="absolute top-8 bottom-8 right-full mr-6 w-64">
        <DateSidebar
          dates={availableDates}
          selectedDate={date}
          onSelectDate={(newDate) => {
            setSelected(null);
            setDate(newDate);
          }}
        />
      </div>

      <Header />

      {selected ? (
        <BriefingDetail briefing={selected} onBack={() => setSelected(null)} />
      ) : (
        <>
          <FilterBar topic={topic} onTopicChange={setTopic} />
          {loading && <p className="text-gray-500">Loading briefings…</p>}
          {error && <p className="text-red-600">Failed to load briefings: {error}</p>}
          {!loading && !error && <BriefingList briefings={briefings} onSelect={setSelected} />}
        </>
      )}
    </div>
  );
}

export default App;
