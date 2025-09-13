# server/ml_orders_forecast_api/__init__.py
# Expose le blueprint sous un nom stable pour app.py
from .service import bp as bp_orders_forecast

__all__ = ["bp_orders_forecast"]
