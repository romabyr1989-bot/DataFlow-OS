"""DataFlow OS Python SDK.

Top-level imports are lazy: importing the package does not pull in
optional dependencies (`requests`, `pandas`). Submodules that need
them raise an informative ImportError when accessed.

Examples:
    from dataflow_os import Client                    # needs requests
    from dataflow_os.airflow_import import main       # stdlib only
"""
from .exceptions import AuthError, PermissionError, TableNotFoundError, DataFlowError

__all__ = [
    "Client",
    "Table",
    "Pipeline",
    "AuthError",
    "PermissionError",
    "TableNotFoundError",
    "DataFlowError",
]
__version__ = "0.1.0"


def __getattr__(name):
    """Lazy attribute access — only imports submodule when used."""
    if name == "Client":
        from .client import Client
        return Client
    if name == "Table":
        from .table import Table
        return Table
    if name == "Pipeline":
        from .pipeline import Pipeline
        return Pipeline
    raise AttributeError(f"module 'dataflow_os' has no attribute {name!r}")
