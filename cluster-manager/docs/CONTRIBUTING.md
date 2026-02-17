# Contributing Guide

## Development Setup

### Prerequisites

- Python 3.10+
- Node.js 18+
- Databricks CLI
- uv (Python package manager)

### Initial Setup

```bash
# Clone repository
git clone https://github.com/your-org/cluster-manager.git
cd cluster-manager

# Setup Python environment
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"

# Setup Node environment
cd cluster_manager/ui
npm install
cd ../..
```

---

## Project Structure

```
cluster-manager/
├── databricks.yml              # DABs configuration
├── app.yaml                    # App runtime config
├── pyproject.toml              # Python project config
├── cluster_manager/            # Main Python package
│   ├── __init__.py
│   ├── _metadata.py            # Version and constants
│   ├── __dist__/               # Built frontend (auto-generated)
│   ├── backend/
│   │   ├── app.py              # FastAPI entry point
│   │   ├── core.py             # Core infrastructure
│   │   ├── models.py           # Pydantic models
│   │   └── routers/            # API endpoints
│   │       ├── clusters.py
│   │       ├── billing.py
│   │       ├── metrics.py
│   │       ├── policies.py
│   │       ├── optimization.py
│   │       └── workspace.py
│   └── ui/                     # React frontend
│       ├── package.json
│       ├── vite.config.ts
│       ├── tailwind.config.js
│       ├── routes/             # TanStack Router pages
│       ├── components/         # React components
│       └── lib/                # Utilities and API client
└── docs/                       # Documentation
```

---

## Development Workflow

### Running Locally

```bash
# Terminal 1: Backend
cd cluster_manager/backend
uvicorn app:app --reload --port 8000

# Terminal 2: Frontend
cd cluster_manager/ui
npm run dev
```

### Code Style

#### Python

- Use type hints for all function signatures
- Follow PEP 8 style guide
- Use Google-style docstrings
- Run `basedpyright` for type checking

```python
def get_cluster(
    cluster_id: str,
    ws: Dependency.Client,
) -> ClusterDetail:
    """Get detailed information about a specific cluster.

    Args:
        cluster_id: Unique cluster identifier.
        ws: Databricks WorkspaceClient instance.

    Returns:
        ClusterDetail with full cluster configuration.

    Raises:
        HTTPException: If cluster not found (404).
    """
```

#### TypeScript/React

- Use functional components with hooks
- Use TanStack Query for data fetching
- Use Tailwind CSS for styling
- Follow shadcn/ui patterns for components

```typescript
function ClusterCard({ cluster }: { cluster: ClusterSummary }) {
  const startCluster = useStartCluster();

  return (
    <Card>
      <CardHeader>
        <CardTitle>{cluster.cluster_name}</CardTitle>
      </CardHeader>
      {/* ... */}
    </Card>
  );
}
```

---

## Adding New Features

### Adding a New API Endpoint

1. **Define Pydantic models** in `models.py`:

```python
class NewFeatureResponse(BaseModel):
    """Response for new feature."""
    data: str
    count: int
```

2. **Create router** in `routers/new_feature.py`:

```python
from fastapi import APIRouter
from ..core import Dependency
from ..models import NewFeatureResponse

router = APIRouter(prefix="/api/new-feature", tags=["new-feature"])

@router.get("", response_model=NewFeatureResponse)
def get_new_feature(ws: Dependency.Client) -> NewFeatureResponse:
    """Get new feature data."""
    # Implementation
    return NewFeatureResponse(data="example", count=42)
```

3. **Register router** in `routers/__init__.py`:

```python
from .new_feature import router as new_feature_router

__all__ = [..., "new_feature_router"]
```

4. **Add to app** in `app.py`:

```python
from .routers import new_feature_router

app = create_app(routers=[..., new_feature_router])
```

### Adding a New UI Page

1. **Create route file** in `routes/_sidebar/new-page.tsx`:

```typescript
import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/_sidebar/new-page")({
  component: NewPage,
});

function NewPage() {
  return (
    <div className="space-y-4">
      <h1 className="text-2xl font-bold">New Page</h1>
      {/* Content */}
    </div>
  );
}
```

2. **Add navigation** in `_sidebar.tsx`:

```typescript
const navItems = [
  // ... existing items
  { to: "/new-page", icon: NewIcon, label: "New Page" },
];
```

3. **Rebuild frontend**:

```bash
cd cluster_manager/ui
npm run build
```

---

## Common Patterns

### Databricks SDK Attribute Access

Always use `getattr` for optional attributes:

```python
# WRONG - may raise AttributeError
last_activity = cluster.last_activity_time

# CORRECT
last_activity = getattr(cluster, 'last_activity_time', None)
```

### Pydantic Model Inheritance

Don't duplicate fields when inheriting:

```python
class ClusterSummary(BaseModel):
    cluster_id: str
    policy_id: str | None = None  # Defined here

class ClusterDetail(ClusterSummary):
    # policy_id inherited - don't add again!
    terminated_time: datetime | None = None
```

### TanStack Router Navigation

Use programmatic navigation in dropdown menus:

```typescript
// WRONG
<Link to="/path"><DropdownMenuItem>Click</DropdownMenuItem></Link>

// CORRECT
const navigate = useNavigate();
<DropdownMenuItem onClick={() => navigate({ to: "/path" })}>
  Click
</DropdownMenuItem>
```

### URL Search Parameters

```typescript
export const Route = createFileRoute("/_sidebar/clusters/")({
  component: ClustersPage,
  validateSearch: (search: Record<string, unknown>) => ({
    filter: (search.filter as string) || undefined,
  }),
});

// In component
const { filter } = Route.useSearch();
```

---

## Testing

### Backend Testing

```bash
# Run type checking
uv run basedpyright

# Test endpoints manually
curl http://localhost:8000/api/health
curl http://localhost:8000/api/clusters | jq .
```

### Frontend Testing

```bash
cd cluster_manager/ui

# Type checking
npm run typecheck

# Build verification
npm run build
```

---

## Deployment

### Pre-Deployment Checklist

1. **Build frontend**:
   ```bash
   cd cluster_manager/ui
   npm run build
   ```

2. **Update version** in `_sidebar.tsx`:
   ```typescript
   <span className="text-xs text-muted-foreground">v1.6.0</span>
   ```

3. **Verify `__dist__` not in `.gitignore`**

4. **Deploy**:
   ```bash
   databricks bundle deploy -t dev
   databricks apps stop cluster-manager
   databricks apps start cluster-manager
   ```

---

## Git Workflow

### Branch Naming

- `feature/description` - New features
- `fix/description` - Bug fixes
- `docs/description` - Documentation updates

### Commit Messages

Follow conventional commits:

```
feat: add policy detail dialog to clusters page
fix: handle missing cluster attributes with getattr
docs: add API reference documentation
refactor: extract cluster card component
```

### Pull Request Process

1. Create feature branch from `main`
2. Make changes and test locally
3. Build frontend if UI changes
4. Create PR with description
5. Request review
6. Merge after approval

---

## Documentation

### When to Update Docs

- New API endpoints → Update `docs/API.md`
- Architecture changes → Update `docs/ARCHITECTURE.md`
- Deployment changes → Update `docs/DEPLOYMENT.md`
- New optimization checks → Update `docs/OPTIMIZATION_STRATEGIES.md`

### Documentation Style

- Use Markdown for all docs
- Include code examples
- Add Mermaid diagrams for complex flows
- Keep README focused on administrators
- Put technical details in separate docs
