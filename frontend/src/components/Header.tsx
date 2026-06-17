export function Header() {
  const dateLabel = new Date().toLocaleDateString("en-US", {
    month: "long",
    day: "numeric",
    year: "numeric",
  });

  return (
    <div className="bg-gradient-to-br from-[#16425b] to-[#2f6690] rounded-2xl px-8 py-8 text-center mb-8 shadow-lg shadow-[#16425b]/15">
      <h1 className="text-white text-2xl font-bold tracking-tight">NYC Local Daily News Brief</h1>
      <p className="text-[#81c3d7] text-sm mt-2">
        For Council Member Virginia Maloney's Office — District 4, Manhattan
      </p>
      <p className="text-[#16425b] bg-[#d9dcd6] inline-block rounded-full px-3 py-1 text-xs font-semibold uppercase tracking-wider mt-3">
        {dateLabel}
      </p>
    </div>
  );
}
