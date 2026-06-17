import { useEffect, useState } from "react";
import { fetchBriefings, type Briefing } from "./api";
import { Header } from "./components/Header";
import { FilterBar } from "./components/FilterBar";
import { BriefingList } from "./components/BriefingList";
import { BriefingDetail } from "./components/BriefingDetail";

function App() {
  const [topic, setTopic] = useState("");
  const [date, setDate] = useState("");
  const [briefings, setBriefings] = useState<Briefing[]>([]);
  const [selected, setSelected] = useState<Briefing | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    setError(null);
    fetchBriefings({ topic, date })
      .then(setBriefings)
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [topic, date]);

  return (
    <div className="max-w-3xl mx-auto px-4 py-8">
      <Header />

      {selected ? (
        <BriefingDetail briefing={selected} onBack={() => setSelected(null)} />
      ) : (
        <>
          <FilterBar topic={topic} date={date} onTopicChange={setTopic} onDateChange={setDate} />
          {loading && <p className="text-gray-500">Loading briefings…</p>}
          {error && <p className="text-red-600">Failed to load briefings: {error}</p>}
          {!loading && !error && <BriefingList briefings={briefings} onSelect={setSelected} />}
        </>
      )}
    </div>
  );
}

export default App;
