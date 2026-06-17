export function Header() {
  const dateLabel = new Date().toLocaleDateString("en-US", {
    month: "long",
    day: "numeric",
    year: "numeric",
  });

  return (
    <div className="bg-gradient-to-br from-[#0f0f17] via-[#1e1b3a] to-[#4338ca] rounded-2xl px-8 py-8 text-center mb-8 shadow-xl shadow-[#4338ca]/15">
      <h1 className="text-white text-2xl font-bold tracking-tight">NYC Local Daily News Brief</h1>
      <p className="text-[#b4b4e0] text-sm mt-2">
        For Council Member Virginia Maloney's Office — District 4, Manhattan
      </p>
      <p className="text-[#4338ca] bg-white inline-block rounded-full px-3 py-1 text-xs font-semibold uppercase tracking-wider mt-3">
        {dateLabel}
      </p>
    </div>
  );
}
