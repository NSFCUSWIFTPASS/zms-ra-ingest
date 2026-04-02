"""Simple HTTP server that serves mock ODS data for testing."""

import datetime
import json
from http.server import BaseHTTPRequestHandler, HTTPServer


# Generate a couple of observations starting in the near future
def _make_observations():
    now = datetime.datetime.now(datetime.UTC)
    return {
        "ods_data": [
            {
                "site_id": "ATA",
                "site_lat_deg": "40.817431",
                "site_lon_deg": "-121.470736",
                "site_el_m": "1019.222",
                "src_id": "ASP",
                "corr_integ_time_sec": 1,
                "src_ra_j2000_deg": 189.585,
                "src_dec_j2000_deg": -4.128,
                "src_start_utc": (now + datetime.timedelta(hours=1)).strftime(
                    "%Y-%m-%dT%H:%M:%S"
                ),
                "src_end_utc": (now + datetime.timedelta(hours=2)).strftime(
                    "%Y-%m-%dT%H:%M:%S"
                ),
                "slew_sec": 30,
                "trk_rate_dec_deg_per_sec": 0,
                "trk_rate_ra_deg_per_sec": 0,
                "freq_lower_hz": 1990000000,
                "freq_upper_hz": 1995000000,
                "version": "v1.0.0",
                "dish_diameter_m": 6.1,
                "subarray": 0,
            },
            {
                "site_id": "ATA",
                "site_lat_deg": "40.817431",
                "site_lon_deg": "-121.470736",
                "site_el_m": "1019.222",
                "src_id": "SETI-survey",
                "corr_integ_time_sec": 1,
                "src_ra_j2000_deg": 100.0,
                "src_dec_j2000_deg": 20.0,
                "src_start_utc": (now + datetime.timedelta(hours=3)).strftime(
                    "%Y-%m-%dT%H:%M:%S"
                ),
                "src_end_utc": (now + datetime.timedelta(hours=5)).strftime(
                    "%Y-%m-%dT%H:%M:%S"
                ),
                "slew_sec": 30,
                "trk_rate_dec_deg_per_sec": 0,
                "trk_rate_ra_deg_per_sec": 0,
                "freq_lower_hz": 1400000000,
                "freq_upper_hz": 1420000000,
                "version": "v1.0.0",
                "dish_diameter_m": 6.1,
                "subarray": 1,
            },
        ]
    }


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(_make_observations()).encode())

    def log_message(self, format, *args):
        print(f"[mock-ods] {args[0]}")


if __name__ == "__main__":
    port = 9999
    server = HTTPServer(("0.0.0.0", port), Handler)
    print(f"Mock ODS server running on http://localhost:{port}/ods.json")
    server.serve_forever()
