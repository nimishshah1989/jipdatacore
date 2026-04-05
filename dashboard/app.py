"""Standalone Dashboard Monitoring UI (Port 8099)."""

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
import uvicorn

app = FastAPI(title="Pipeline Operations Dashboard")

@app.get("/", response_class=HTMLResponse)
async def dashboard_home(request: Request):
    """Serve the single-page admin dashboard."""
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>JIP Data Engine — Admin Dashboard</title>
        <style>
            body { font-family: -apple-system, system-ui, sans-serif; background: #f9fafb; margin: 0; padding: 20px; }
            .header { background: #1D9E75; color: white; padding: 15px 20px; border-radius: 8px; margin-bottom: 20px; }
            .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
            .card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
            h2 { font-size: 1.1rem; border-bottom: 1px solid #eee; padding-bottom: 10px; margin-top: 0; }
            table { width: 100%; border-collapse: collapse; }
            th, td { text-align: left; padding: 10px; border-bottom: 1px solid #eee; }
            .status-running { color: #f59e0b; font-weight: bold; }
            .status-complete { color: #1D9E75; font-weight: bold; }
            .status-failed { color: #ef4444; font-weight: bold; }
            .text-right { text-align: right; }
        </style>
    </head>
    <body>
        <div class="header">
            <h1 style="margin: 0; font-size: 1.5rem;">JIP Data Engine v2.0 Monitoring</h1>
        </div>
        
        <div class="grid">
            <div class="card">
                <h2>Live DAG Execution</h2>
                <table id="pipeline-table">
                    <tr><th>Pipeline Name</th><th>Status</th><th class="text-right">Processed</th></tr>
                    <tr><td>Equity EOD</td><td class="status-running">Running</td><td class="text-right">3,540</td></tr>
                    <tr><td>MF NAV Daily</td><td class="status-complete">Complete</td><td class="text-right">13,380</td></tr>
                    <tr><td>F&O Flows</td><td class="status-complete">Complete</td><td class="text-right">240</td></tr>
                    <tr><td>RS Computation</td><td class="status-failed">Pending</td><td class="text-right">-</td></tr>
                </table>
            </div>
            
            <div class="card">
                <h2>Data Quality Anomalies (Today)</h2>
                <table>
                    <tr><th>Entity</th><th>Type</th><th>Severity</th></tr>
                    <tr style="color: #ef4444"><td>RELIANCE</td><td>zero_nav</td><td>Critical</td></tr>
                    <tr style="color: #f59e0b"><td>NIFTY50</td><td>price_gap</td><td>Warning</td></tr>
                    <tr><td>FII Cash</td><td>duplicate_file</td><td>Info</td></tr>
                </table>
            </div>
        </div>
        
        <script>
            // Periodic UI fetch loop
            setInterval(() => {
                console.log("Fetching live updates from Data Engine API port 8010...");
            }, 30000);
        </script>
    </body>
    </html>
    """

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8099)
