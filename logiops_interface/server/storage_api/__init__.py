from __future__ import annotations
from flask import Blueprint

bp = Blueprint("storage_api", __name__, url_prefix="/api/storage")

# En important ces modules, leurs routes s’enregistrent sur le blueprint.
from . import kpis  # noqa: F401
from . import zones  # noqa: F401
from . import hotspots  # noqa: F401
from . import warehouse_map  # noqa: F401
from . import slotting  # noqa: F401
