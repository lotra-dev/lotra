# Lotra v1.21.17 — bug fixes only

- SoFi security-wide order rejections are written to Activity Log and remaining orders for that ticker are skipped for the current run.
- Other tickers and brokers continue processing instead of waiting behind rejected SoFi orders.
- Account-specific SoFi failures remain isolated and retryable where appropriate.
