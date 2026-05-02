# Balfund Renko Options Scalper v1.0

**NIFTY ATM Options Scalper using Renko Brick Signals**

Builds live Renko charts on NIFTY ATM CE & PE option premiums via Dhan WebSocket, and executes trades on green brick closes with a fixed 1:4 risk-reward (SL 2 pts / TP 8 pts).

---

## Strategy

| Parameter        | Value                                         |
|------------------|-----------------------------------------------|
| Instrument       | NIFTY ATM CE & PE (nearest weekly expiry)     |
| Renko Source     | Option premium LTP (tick-by-tick)             |
| Brick Size       | Configurable (default: 1 point)               |
| Entry            | Buy on green brick close                      |
| Target           | +8 points from entry                          |
| Stop Loss        | -2 points from entry                          |
| Risk:Reward      | 1:4                                           |
| Re-entry         | Wait for next fresh green brick after exit     |
| ATM Selection    | Auto from NIFTY spot (nearest 50 multiple)    |
| ATM Shift        | Only when both CE & PE trades are flat         |
| Session          | 09:15 – 15:30 IST                             |
| Max Trades/Day   | Configurable (default: 10)                    |
| Mode             | Paper / Live toggle                           |

## Features

- **Dual Renko Charts** — live CE and PE Renko visualization side-by-side
- **Tick-by-tick TP/SL** — monitored via WebSocket LTP, not polling
- **Auto ATM Strike** — resolves from Dhan instrument master, shifts dynamically
- **Paper/Live Toggle** — test without risking capital
- **Trade Log** — full entry/exit history with P&L
- **Daily Reset** — automatic counter reset at midnight IST
- **Session Guard** — trades only during market hours

## Quick Start

### Run from Source
```bash
pip install -r requirements.txt
# Create .env with DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN
python balfund_renko_options_scalper.py
```

### Run EXE
1. Download `BalfundRenkoScalper.exe` from [Releases](../../releases)
2. Place `.env` file alongside the EXE (or enter credentials in GUI)
3. Double-click to run

## Building EXE Locally

```bash
pip install -r requirements.txt pyinstaller
pyinstaller balfund_renko_options_scalper.spec --noconfirm
# Output: dist/BalfundRenkoScalper.exe
```

## GitHub Actions

Push to `main` → automatic EXE build uploaded as artifact.
Push a tag (`v1.0`, `v1.1`, etc.) → automatic GitHub Release with EXE attached.

## Environment Variables

| Variable            | Description          |
|---------------------|----------------------|
| `DHAN_CLIENT_ID`    | Dhan client ID       |
| `DHAN_ACCESS_TOKEN` | Dhan access token    |

These can be set in a `.env` file or entered directly in the GUI.

## Project Structure

```
├── .github/workflows/build.yml        # GitHub Actions CI/CD
├── balfund_renko_options_scalper.py    # Main application
├── balfund_renko_options_scalper.spec  # PyInstaller spec
├── requirements.txt                   # Python dependencies
├── .env.example                       # Environment template
└── README.md
```

## Dhan API Notes

- WebSocket v2 for live ticks (NIFTY spot + CE + PE)
- REST v2 for order placement (market orders, INTRADAY product)
- Instrument master CSV for strike resolution
- Ensure your Dhan API IP is whitelisted

---

**Balfund Trading Pvt Ltd** | info@balfund.com
