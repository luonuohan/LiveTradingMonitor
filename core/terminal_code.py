python -m streamlit run ui/app.py
PYTHONUNBUFFERED=1 python -m services.main
echo '{"ts_ms":0,"last_prices":{},"positions":{},"realized_pnl":0.0,"unrealized_pnl":0.0,"latency_ms":0.0,"var_90":0.0,"transaction_costs":0.0,"risk":{}}' > storage/state.json
