export function Header() {
  const dateLabel = new Date().toLocaleDateString("en-US", {
    month: "long",
    day: "numeric",
    year: "numeric",
  });

  return (
    <div className="bg-gradient-to-br from-[#1a1a2e] to-[#16213e] rounded-2xl px-8 py-8 text-center mb-8 shadow-lg shadow-[#1a1a2e]/10">
      <h1 className="text-white text-2xl font-bold tracking-tight">NYC Local Daily News Brief</h1>
      <p className="text-[#a0a0c0] text-sm mt-2">
        For Council Member Virginia Maloney's Office — District 4, Manhattan
      </p>
      <p className="text-[#e94560] text-xs font-medium uppercase tracking-wider mt-3">{dateLabel}</p>
    </div>
  );
}
