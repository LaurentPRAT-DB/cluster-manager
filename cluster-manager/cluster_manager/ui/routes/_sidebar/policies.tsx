import { createFileRoute, Link } from "@tanstack/react-router";
import {
  AlertCircle,
  Calendar,
  ChevronRight,
  Loader2,
  RefreshCw,
  Shield,
  Star,
  User,
} from "lucide-react";

import { usePolicies } from "@/lib/api";
import { formatDateTime } from "@/lib/utils";

function PoliciesPage() {
  const { data: policies, isLoading, error, refetch } = usePolicies();

  if (error) {
    return (
      <div className="space-y-6">
        <div>
          <h1 className="text-2xl font-bold">Cluster Policies</h1>
          <p className="text-muted-foreground">Manage cluster configuration policies</p>
        </div>
        <div className="flex flex-col items-center justify-center py-12">
          <AlertCircle className="h-12 w-12 text-destructive mb-4" />
          <h2 className="text-lg font-semibold mb-2">Failed to load policies</h2>
          <p className="text-muted-foreground mb-4">{error.message}</p>
          <button
            onClick={() => refetch()}
            className="flex items-center gap-2 px-4 py-2 bg-primary text-primary-foreground rounded-lg"
          >
            <RefreshCw size={16} />
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Cluster Policies</h1>
          <p className="text-muted-foreground">
            View cluster policies that govern cluster configurations
          </p>
        </div>
        <button
          onClick={() => refetch()}
          className="flex items-center gap-2 px-3 py-2 text-sm bg-secondary hover:bg-secondary/80 rounded-lg transition-colors"
        >
          <RefreshCw size={16} />
          Refresh
        </button>
      </div>

      {/* Policy List */}
      {isLoading ? (
        <div className="flex items-center justify-center py-12">
          <Loader2 className="h-8 w-8 animate-spin text-primary" />
        </div>
      ) : policies && policies.length > 0 ? (
        <div className="grid gap-4">
          {policies.map((policy) => (
            <div
              key={policy.policy_id}
              className="bg-card rounded-lg border p-5 hover:border-primary/50 transition-colors"
            >
              <div className="flex items-start justify-between">
                <div className="flex items-start gap-4 flex-1">
                  <div className="p-2 bg-primary/10 rounded-lg">
                    <Shield className="h-5 w-5 text-primary" />
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2">
                      <h3 className="font-semibold">{policy.name}</h3>
                      {policy.is_default && (
                        <span className="inline-flex items-center gap-1 px-2 py-0.5 bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-400 rounded-full text-xs">
                          <Star size={12} />
                          Default
                        </span>
                      )}
                    </div>
                    {policy.description && (
                      <p className="text-sm text-muted-foreground mt-1">{policy.description}</p>
                    )}
                    <div className="flex flex-wrap gap-4 mt-3 text-sm text-muted-foreground">
                      {policy.creator_user_name && (
                        <div className="flex items-center gap-1.5">
                          <User size={14} />
                          <span>{policy.creator_user_name}</span>
                        </div>
                      )}
                      {policy.created_at_timestamp && (
                        <div className="flex items-center gap-1.5">
                          <Calendar size={14} />
                          <span>{formatDateTime(policy.created_at_timestamp)}</span>
                        </div>
                      )}
                    </div>
                  </div>
                </div>
                <Link
                  to="/clusters"
                  search={{ policy: policy.policy_id }}
                  className="p-2 hover:bg-muted rounded-lg transition-colors"
                  title="View clusters using this policy"
                >
                  <ChevronRight size={20} />
                </Link>
              </div>

              {/* Policy Definition Preview */}
              {policy.definition && (
                <details className="mt-4 group">
                  <summary className="text-sm text-muted-foreground cursor-pointer hover:text-foreground transition-colors">
                    View policy definition
                  </summary>
                  <pre className="mt-2 p-3 bg-muted rounded-lg text-xs overflow-x-auto">
                    {(() => {
                      try {
                        return JSON.stringify(JSON.parse(policy.definition), null, 2);
                      } catch {
                        return policy.definition;
                      }
                    })()}
                  </pre>
                </details>
              )}
            </div>
          ))}
        </div>
      ) : (
        <div className="text-center py-12">
          <Shield className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
          <h2 className="text-lg font-semibold mb-2">No policies found</h2>
          <p className="text-muted-foreground">
            Create cluster policies in your Databricks workspace to see them here.
          </p>
        </div>
      )}
    </div>
  );
}

export const Route = createFileRoute("/_sidebar/policies")({
  component: PoliciesPage,
});
