import { X, Shield, User, Calendar, Check, FileJson } from "lucide-react";
import { ClusterPolicyDetail } from "@/lib/api";

interface PolicyDetailDialogProps {
  policy: ClusterPolicyDetail | null | undefined;
  isLoading: boolean;
  isOpen: boolean;
  onClose: () => void;
}

export function PolicyDetailDialog({
  policy,
  isLoading,
  isOpen,
  onClose,
}: PolicyDetailDialogProps) {
  if (!isOpen) return null;

  return (
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 bg-black/50 z-50"
        onClick={onClose}
      />

      {/* Dialog */}
      <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
        <div
          className="bg-background rounded-lg shadow-xl max-w-2xl w-full max-h-[80vh] overflow-hidden"
          onClick={(e) => e.stopPropagation()}
        >
          {/* Header */}
          <div className="flex items-center justify-between px-6 py-4 border-b">
            <div className="flex items-center gap-3">
              <div className="p-2 bg-primary/10 rounded-lg">
                <Shield className="h-5 w-5 text-primary" />
              </div>
              <div>
                <h2 className="text-lg font-semibold">Policy Details</h2>
                {policy && (
                  <p className="text-sm text-muted-foreground">{policy.name}</p>
                )}
              </div>
            </div>
            <button
              onClick={onClose}
              className="p-2 rounded-lg hover:bg-muted transition-colors"
            >
              <X className="h-5 w-5" />
            </button>
          </div>

          {/* Content */}
          <div className="p-6 overflow-y-auto max-h-[calc(80vh-120px)]">
            {isLoading ? (
              <div className="flex items-center justify-center py-12">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary" />
              </div>
            ) : policy ? (
              <div className="space-y-6">
                {/* Basic Info */}
                <div className="space-y-4">
                  {policy.description && (
                    <div>
                      <label className="text-sm font-medium text-muted-foreground">Description</label>
                      <p className="mt-1 text-sm">{policy.description}</p>
                    </div>
                  )}

                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <label className="text-sm font-medium text-muted-foreground flex items-center gap-1.5">
                        <User className="h-3.5 w-3.5" />
                        Created By
                      </label>
                      <p className="mt-1 text-sm">{policy.creator_user_name || "-"}</p>
                    </div>
                    <div>
                      <label className="text-sm font-medium text-muted-foreground flex items-center gap-1.5">
                        <Calendar className="h-3.5 w-3.5" />
                        Created At
                      </label>
                      <p className="mt-1 text-sm">
                        {policy.created_at_timestamp
                          ? new Date(policy.created_at_timestamp).toLocaleDateString()
                          : "-"}
                      </p>
                    </div>
                  </div>

                  <div className="flex items-center gap-4">
                    {policy.is_default && (
                      <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400">
                        <Check className="h-3 w-3" />
                        Default Policy
                      </span>
                    )}
                    {policy.max_clusters_per_user && (
                      <span className="text-sm text-muted-foreground">
                        Max {policy.max_clusters_per_user} clusters per user
                      </span>
                    )}
                  </div>
                </div>

                {/* Policy Definition */}
                {policy.definition_json && Object.keys(policy.definition_json).length > 0 && (
                  <div>
                    <label className="text-sm font-medium text-muted-foreground flex items-center gap-1.5 mb-2">
                      <FileJson className="h-3.5 w-3.5" />
                      Policy Definition
                    </label>
                    <div className="bg-muted rounded-lg p-4 overflow-x-auto">
                      <pre className="text-xs font-mono whitespace-pre-wrap">
                        {JSON.stringify(policy.definition_json, null, 2)}
                      </pre>
                    </div>
                  </div>
                )}

                {/* Policy Family */}
                {policy.policy_family_id && (
                  <div>
                    <label className="text-sm font-medium text-muted-foreground">Policy Family</label>
                    <p className="mt-1 text-sm font-mono text-muted-foreground">
                      {policy.policy_family_id}
                    </p>
                  </div>
                )}
              </div>
            ) : (
              <div className="text-center py-12 text-muted-foreground">
                Policy not found
              </div>
            )}
          </div>
        </div>
      </div>
    </>
  );
}
