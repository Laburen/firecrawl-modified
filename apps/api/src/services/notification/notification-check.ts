import { isEnterpriseTeamCreatedAfterRateLimitChange } from "../subscription/enterprise-check";

export async function shouldSendConcurrencyLimitNotification(
  team_id: string,
): Promise<boolean> {
  return false
  const isEnterprise =
    await isEnterpriseTeamCreatedAfterRateLimitChange(team_id);
  return !isEnterprise;
}
