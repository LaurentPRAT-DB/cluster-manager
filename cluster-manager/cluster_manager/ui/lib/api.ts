import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

// Types matching backend models
export interface ClusterSummary {
  cluster_id: string;
  cluster_name: string;
  state: string;
  creator_user_name: string | null;
  node_type_id: string | null;
  driver_node_type_id: string | null;
  num_workers: number | null;
  autoscale: { min_workers: number; max_workers: number } | null;
  spark_version: string | null;
  cluster_source: string | null;
  start_time: string | null;
  last_activity_time: string | null;
  uptime_minutes: number;
  estimated_dbu_per_hour: number;
}

export interface ClusterDetail extends ClusterSummary {
  terminated_time: string | null;
  termination_reason: string | null;
  state_message: string | null;
  default_tags: Record<string, string>;
  custom_tags: Record<string, string>;
  spark_conf: Record<string, string>;
  spark_env_vars: Record<string, string>;
  policy_id: string | null;
  data_security_mode: string | null;
}

export interface ClusterEvent {
  cluster_id: string;
  timestamp: string;
  event_type: string;
  details: Record<string, unknown>;
}

export interface ClusterMetricsSummary {
  total_clusters: number;
  running_clusters: number;
  pending_clusters: number;
  terminated_clusters: number;
  total_running_workers: number;
  estimated_hourly_dbu: number;
}

export interface IdleClusterAlert {
  cluster_id: string;
  cluster_name: string;
  idle_duration_minutes: number;
  estimated_wasted_dbu: number;
  recommendation: string;
}

export interface OptimizationRecommendation {
  cluster_id: string;
  cluster_name: string;
  issue: string;
  recommendation: string;
  potential_savings: string;
  priority: string;
}

export interface BillingSummary {
  total_dbu: number;
  estimated_cost_usd: number;
  period_start: string;
  period_end: string;
  currency: string;
}

export interface ClusterBillingUsage {
  cluster_id: string;
  cluster_name: string | null;
  total_dbu: number;
  estimated_cost_usd: number;
  usage_date_start: string;
  usage_date_end: string;
}

export interface BillingTrend {
  date: string;
  dbu: number;
  estimated_cost_usd: number;
}

export interface TopConsumer {
  cluster_id: string;
  cluster_name: string | null;
  total_dbu: number;
  estimated_cost_usd: number;
  percentage_of_total: number;
}

export interface ClusterPolicySummary {
  policy_id: string;
  name: string;
  definition: string | null;
  description: string | null;
  creator_user_name: string | null;
  created_at_timestamp: string | null;
  is_default: boolean;
}

export interface ClusterActionResponse {
  success: boolean;
  message: string;
  cluster_id: string;
}

// Optimization types
export interface OptimizationSummary {
  total_clusters_analyzed: number;
  oversized_clusters: number;
  underutilized_clusters: number;
  total_potential_monthly_savings: number;
  recommendations_count: number;
  last_analysis_time: string;
}

export interface OversizedClusterAnalysis {
  cluster_id: string;
  cluster_name: string;
  cluster_type: string;
  current_workers: number;
  avg_efficiency_score: number;
  avg_daily_dbu: number;
  recommended_workers: number;
  potential_dbu_savings: number;
  potential_cost_savings: number;
}

export interface JobClusterRecommendation {
  source_cluster_id: string;
  source_cluster_name: string;
  target_cluster_id: string;
  target_cluster_name: string;
  job_count: number;
  reason: string;
  estimated_savings: string;
}

export interface ScheduleOptimizationRecommendation {
  cluster_id: string;
  cluster_name: string;
  current_auto_terminate_minutes: number | null;
  recommended_auto_terminate_minutes: number;
  avg_idle_time_per_day_minutes: number;
  peak_usage_hours: number[];
  reason: string;
}

export interface MetricsCollectionResponse {
  success: boolean;
  message: string;
  clusters_processed: number;
  metrics_persisted: boolean;
}

// API functions
async function fetchApi<T>(url: string, options?: RequestInit): Promise<T> {
  const response = await fetch(url, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...options?.headers,
    },
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: "Unknown error" }));
    throw new Error(error.detail || `HTTP ${response.status}`);
  }

  return response.json();
}

// Query hooks
export function useClusters(state?: string) {
  const queryParams = state ? `?state=${state}` : "";
  return useQuery({
    queryKey: ["clusters", state],
    queryFn: () => fetchApi<ClusterSummary[]>(`/api/clusters${queryParams}`),
    refetchInterval: 30000, // Refresh every 30 seconds
  });
}

export function useCluster(clusterId: string) {
  return useQuery({
    queryKey: ["cluster", clusterId],
    queryFn: () => fetchApi<ClusterDetail>(`/api/clusters/${clusterId}`),
    enabled: !!clusterId,
    refetchInterval: 10000, // Refresh every 10 seconds
  });
}

export function useClusterEvents(clusterId: string, limit = 50) {
  return useQuery({
    queryKey: ["cluster-events", clusterId, limit],
    queryFn: () =>
      fetchApi<{ events: ClusterEvent[]; total_count: number }>(
        `/api/clusters/${clusterId}/events?limit=${limit}`
      ),
    enabled: !!clusterId,
  });
}

export function useMetricsSummary() {
  return useQuery({
    queryKey: ["metrics-summary"],
    queryFn: () => fetchApi<ClusterMetricsSummary>("/api/metrics/summary"),
    refetchInterval: 30000,
  });
}

export function useIdleClusters() {
  return useQuery({
    queryKey: ["idle-clusters"],
    queryFn: () => fetchApi<IdleClusterAlert[]>("/api/metrics/idle-clusters"),
    refetchInterval: 60000,
  });
}

export function useRecommendations() {
  return useQuery({
    queryKey: ["recommendations"],
    queryFn: () => fetchApi<OptimizationRecommendation[]>("/api/metrics/recommendations"),
    refetchInterval: 60000,
  });
}

export function useBillingSummary(days = 30) {
  return useQuery({
    queryKey: ["billing-summary", days],
    queryFn: () => fetchApi<BillingSummary>(`/api/billing/summary?days=${days}`),
    refetchInterval: 300000, // 5 minutes
  });
}

export function useBillingByCluster(days = 30, limit = 50) {
  return useQuery({
    queryKey: ["billing-by-cluster", days, limit],
    queryFn: () =>
      fetchApi<ClusterBillingUsage[]>(`/api/billing/by-cluster?days=${days}&limit=${limit}`),
    refetchInterval: 300000,
  });
}

export function useBillingTrend(days = 30) {
  return useQuery({
    queryKey: ["billing-trend", days],
    queryFn: () => fetchApi<BillingTrend[]>(`/api/billing/trend?days=${days}`),
    refetchInterval: 300000,
  });
}

export function useTopConsumers(days = 30, limit = 10) {
  return useQuery({
    queryKey: ["top-consumers", days, limit],
    queryFn: () => fetchApi<TopConsumer[]>(`/api/billing/top-consumers?days=${days}&limit=${limit}`),
    refetchInterval: 300000,
  });
}

export function usePolicies() {
  return useQuery({
    queryKey: ["policies"],
    queryFn: () => fetchApi<ClusterPolicySummary[]>("/api/policies"),
  });
}

// Mutation hooks
export function useStartCluster() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (clusterId: string) =>
      fetchApi<ClusterActionResponse>(`/api/clusters/${clusterId}/start`, { method: "POST" }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["clusters"] });
      queryClient.invalidateQueries({ queryKey: ["metrics-summary"] });
    },
  });
}

export function useStopCluster() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (clusterId: string) =>
      fetchApi<ClusterActionResponse>(`/api/clusters/${clusterId}/stop`, { method: "POST" }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["clusters"] });
      queryClient.invalidateQueries({ queryKey: ["metrics-summary"] });
    },
  });
}

// Optimization hooks
export function useOptimizationSummary() {
  return useQuery({
    queryKey: ["optimization-summary"],
    queryFn: () => fetchApi<OptimizationSummary>("/api/optimization/summary"),
    refetchInterval: 60000,
  });
}

export function useOversizedClusters(minWorkers = 10) {
  return useQuery({
    queryKey: ["oversized-clusters", minWorkers],
    queryFn: () =>
      fetchApi<OversizedClusterAnalysis[]>(`/api/optimization/oversized-clusters?min_workers=${minWorkers}`),
    refetchInterval: 60000,
  });
}

export function useJobRecommendations() {
  return useQuery({
    queryKey: ["job-recommendations"],
    queryFn: () => fetchApi<JobClusterRecommendation[]>("/api/optimization/job-recommendations"),
    refetchInterval: 60000,
  });
}

export function useScheduleRecommendations() {
  return useQuery({
    queryKey: ["schedule-recommendations"],
    queryFn: () =>
      fetchApi<ScheduleOptimizationRecommendation[]>("/api/optimization/schedule-recommendations"),
    refetchInterval: 60000,
  });
}

export function useCollectMetrics() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: () =>
      fetchApi<MetricsCollectionResponse>("/api/optimization/collect-metrics", { method: "POST" }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["optimization-summary"] });
      queryClient.invalidateQueries({ queryKey: ["oversized-clusters"] });
      queryClient.invalidateQueries({ queryKey: ["job-recommendations"] });
      queryClient.invalidateQueries({ queryKey: ["schedule-recommendations"] });
    },
  });
}
