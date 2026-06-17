export function Header() {
  const dateLabel = new Date().toLocaleDateString("en-US", {
    month: "long",
    day: "numeric",
    year: "numeric",
  });

  return (
    <div className="bg-gradient-to-br from-[#283618] to-[#606c38] rounded-2xl px-8 py-8 text-center mb-8 shadow-lg shadow-[#283618]/15">
      <h1 className="text-[#fefae0] text-2xl font-bold tracking-tight">NYC Local Daily News Brief</h1>
      <p className="text-[#dda15e] text-sm mt-2">
        For Council Member Virginia Maloney's Office — District 4, Manhattan
      </p>
      <p className="text-[#bc6c25] bg-[#fefae0] inline-block rounded-full px-3 py-1 text-xs font-semibold uppercase tracking-wider mt-3">
        {dateLabel}
      </p>
    </div>
  );
}
