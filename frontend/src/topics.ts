export const TOPIC_LABELS: Record<string, string> = {
  politics_government: "Politics and Government",
  economy_business: "Economy and Business",
  public_safety: "Public Safety",
  health: "Health",
  education: "Education",
  climate_energy: "Climate and Energy",
  transportation_housing: "Transportation and Housing",
};

export function topicLabel(topic: string): string {
  return TOPIC_LABELS[topic] ?? topic;
}
