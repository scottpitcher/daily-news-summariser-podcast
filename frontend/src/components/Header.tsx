export function Header() {
  const dateLabel = new Date().toLocaleDateString("en-US", {
    month: "long",
    day: "numeric",
    year: "numeric",
  });

  return (
    <div className="bg-white rounded-xl border border-[#e8e5dc] text-center mb-8 px-8 py-8">
      <h1 className="text-[#2b2a28] text-3xl font-semibold tracking-tight">NYC Local Daily News Brief</h1>
      <p className="text-[#79766d] text-sm mt-3">
        For Council Member Virginia Maloney's Office — District 4, Manhattan
      </p>
      <p className="text-[#bd8369] text-xs font-semibold uppercase tracking-widest mt-4">{dateLabel}</p>
    </div>
  );
}
