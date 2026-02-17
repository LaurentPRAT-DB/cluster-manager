# Databricks Cluster Manager

A full-stack Databricks application for administrators to manage clusters with a focus on **cost optimization** and **usage efficiency**.

## Features

- **Cluster Management**: View all clusters, start/stop operations (Safe Mode - no terminate)
- **Cost Analytics**: DBU usage trends, billing data from Unity Catalog system tables
- **Optimization Insights**: Idle cluster alerts, recommendations for cost savings
- **Policy Overview**: View cluster policies and their usage

## Architecture

- **Backend**: FastAPI (Python) using Databricks SDK
- **Frontend**: React + TypeScript with TanStack Router & Query
- **Deployment**: Databricks Asset Bundles (DABS)

## Prerequisites

- Python 3.10+
- Node.js 18+ (or Bun)
- Databricks CLI configured
- Access to Unity Catalog `system.billing.usage` table (for cost analytics)
- SQL Warehouse (for billing queries)

## Project Structure

```
cluster-manager/
├── databricks.yml          # DABS configuration
├── app.yaml                # Databricks App config
├── pyproject.toml          # Python dependencies
└── src/cluster_manager/
    ├── backend/            # FastAPI backend
    │   ├── app.py          # Main app
    │   ├── core.py         # Config & dependencies
    │   ├── models.py       # Pydantic models
    │   └── routers/        # API endpoints
    │       ├── clusters.py
    │       ├── billing.py
    │       ├── metrics.py
    │       └── policies.py
    └── ui/                 # React frontend
        ├── routes/         # Page components
        ├── lib/            # API hooks & utils
        └── styles/         # CSS
```

## Local Development

### Backend

```bash
# Create virtual environment
cd cluster-manager
python -m venv .venv
source .venv/bin/activate  # or `.venv\Scripts\activate` on Windows

# Install dependencies
pip install -e .

# Set environment variables
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=your-token

# Run backend
uvicorn cluster_manager.backend.app:app --reload --port 8000
```

### Frontend

```bash
cd src/cluster_manager/ui

# Install dependencies
npm install  # or bun install

# Run development server
npm run dev  # or bun dev
```

The frontend will be available at `http://localhost:5173` and proxy API requests to the backend.

## Deployment

### Using DABS

```bash
# Validate bundle
databricks bundle validate

# Deploy to dev
databricks bundle deploy -t dev

# Deploy to production
databricks bundle deploy -t prod
```

### Configuration

Set the SQL Warehouse ID for billing queries:

```bash
databricks bundle deploy -t dev -var="sql_warehouse_id=your-warehouse-id"
```

Or set in your environment:
```bash
export CLUSTER_MANAGER_SQL_WAREHOUSE_ID=your-warehouse-id
```

## API Endpoints

### Clusters
- `GET /api/clusters` - List all clusters
- `GET /api/clusters/{id}` - Get cluster details
- `POST /api/clusters/{id}/start` - Start a cluster
- `POST /api/clusters/{id}/stop` - Stop a cluster (Safe Mode)
- `GET /api/clusters/{id}/events` - Get cluster events

### Billing (requires Unity Catalog)
- `GET /api/billing/summary` - DBU usage summary
- `GET /api/billing/by-cluster` - Usage by cluster
- `GET /api/billing/trend` - Daily usage trend
- `GET /api/billing/top-consumers` - Top consuming clusters

### Metrics
- `GET /api/metrics/summary` - Cluster metrics summary
- `GET /api/metrics/idle-clusters` - Idle cluster alerts
- `GET /api/metrics/recommendations` - Optimization recommendations

### Policies
- `GET /api/policies` - List cluster policies
- `GET /api/policies/{id}` - Get policy details
- `GET /api/policies/{id}/usage` - Clusters using policy

## Safe Mode

This application operates in **Safe Mode**, which means:
- Cluster **Start** and **Stop** operations are available
- Cluster **Terminate** (permanent deletion) is **disabled**
- This prevents accidental cluster loss

## License

MIT
