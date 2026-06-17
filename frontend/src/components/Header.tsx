export function Header() {
  const dateLabel = new Date().toLocaleDateString("en-US", {
    month: "long",
    day: "numeric",
    year: "numeric",
  });

  return (
    <div className="bg-[#1a1a2e] rounded-lg px-8 py-7 text-center mb-6">
      <h1 className="text-white text-xl font-semibold tracking-wide">NYC Local Daily News Brief</h1>
      <p className="text-[#a0a0c0] text-sm mt-1.5">
        For Council Member Virginia Maloney's Office — District 4, Manhattan
      </p>
      <p className="text-[#a0a0c0] text-sm mt-1.5">{dateLabel}</p>
    </div>
  );
}
