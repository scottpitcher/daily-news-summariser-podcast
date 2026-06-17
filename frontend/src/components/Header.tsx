export function Header() {
  const dateLabel = new Date().toLocaleDateString("en-US", {
    month: "long",
    day: "numeric",
    year: "numeric",
  });

  return (
    <div className="text-center mb-10 pb-7 border-b border-[#e5e3da]">
      <h1 className="text-[#141413] text-3xl font-semibold tracking-tight">NYC Local Daily News Brief</h1>
      <p className="text-[#6b6862] text-sm mt-3">
        For Council Member Virginia Maloney's Office — District 4, Manhattan
      </p>
      <p className="text-[#cc785c] text-xs font-semibold uppercase tracking-widest mt-4">{dateLabel}</p>
    </div>
  );
}
