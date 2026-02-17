"""Main FastAPI application."""

from databricks.sdk.service.iam import User as UserOut

from .core import Dependency, create_app, create_router
from .models import VersionOut
from .routers import billing_router, clusters_router, metrics_router, policies_router

# Create main router for basic endpoints
main_router = create_router()


@main_router.get("/version", response_model=VersionOut, operation_id="version")
async def version():
    """Get the application version."""
    return VersionOut.from_metadata()


@main_router.get("/current-user", response_model=UserOut, operation_id="currentUser")
def current_user(user_ws: Dependency.UserClient):
    """Get the current authenticated user."""
    return user_ws.current_user.me()


@main_router.get("/health", operation_id="health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy"}


# Create the app with all routers
app = create_app(
    routers=[
        main_router,
        clusters_router,
        metrics_router,
        billing_router,
        policies_router,
    ]
)
