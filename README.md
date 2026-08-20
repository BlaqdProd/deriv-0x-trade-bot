# Deriv 0 → X Setup & Trade Bot

A Deriv trading bot that tracks **0 → X** digit patterns with a built-in Trade Manager.

## How it works

1. Starts in **LOCKED** mode (paper trading only)
2. Watches for the pattern: last digit `0` followed by digit `X`
3. When the same `X` appears again → places a **Digit Differs 0** trade
4. After **2 consecutive paper losses**, the bot **unlocks** and places **one real trade**
5. Immediately locks again after the real trade
6. Win/Loss is determined by the next tick (digit `0` = loss, anything else = win)

## Features

- Paper trading until double loss unlocks real trading
- One real trade per unlock cycle
- Configurable stake and max real trades
- Live terminal UI with stats, tick feed, and trade history
- Saves configuration for next run
- Supports Synthetic Indices, Volatility Indices, Forex, Crypto

## Requirements

- Python 3.8+
- `curl` (usually pre-installed on Windows/macOS/Linux)
- Deriv App ID + API Token (demo or real)

```bash
pip install -r requirements.txt
```

## Quick Start

1. Clone the repo
2. (Optional) Edit `deriv_0x_trade_config.json` with your credentials
3. Run:

```bash
python digit_0_polar_opposite.py
```

On first run the bot will ask for:
- Asset symbol (e.g. `R_10`, `R_100`, `1HZ100V`)
- Decimal places
- Stake size
- Max real trades (0 = unlimited)
- App ID & API Token
- Demo or Real account

Configuration is saved automatically for future runs.

## Disclaimer

This is for educational / testing purposes. Trading involves risk of loss. Use demo accounts first. The authors are not responsible for any financial losses.

## License

MIT
