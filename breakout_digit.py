#!/usr/bin/env python3
"""
DERIV STREAK FOLLOW BOT WITH TRADE MANAGER
Follows breakout chain until setup found: Streak → Breakout → Focus → Trade
Paper trading until double loss unlocks real trades
"""

import asyncio
import websockets
import json
import os
import subprocess
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from collections import deque
import json
import sys

# ============= CONFIGURATION FILE =============
CONFIG_FILE = "deriv_streak_follow_config.json"

# ============= COLOR CODES (Windows compatible) =============
import ctypes
try:
    kernel32 = ctypes.windll.kernel32
    kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
except:
    pass

USE_COLORS = True
if USE_COLORS:
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    CYAN = "\033[96m"
    MAGENTA = "\033[95m"
    BLUE = "\033[94m"
    RESET = "\033[0m"
else:
    RED = ""
    GREEN = ""
    YELLOW = ""
    CYAN = ""
    MAGENTA = ""
    BLUE = ""
    RESET = ""

# ============= DERIV API CONSTANTS =============
REST_BASE = "https://api.derivws.com"
PUBLIC_WS_URL = "wss://api.derivws.com/trading/v1/options/ws/public"

# ============= AUTHENTICATION HELPERS (curl for Windows) =============
def curl_request(method: str, url: str, headers: dict) -> tuple[int, str]:
    """
    Runs a curl request and returns (status_code, response_body).
    Uses curl instead of requests because Deriv's Cloudflare layer
    blocks Python's TLS fingerprint but allows curl's.
    """
    cmd = ["curl", "-s", "-X", method, url, "-w", "\n%{http_code}"]
    for key, value in headers.items():
        cmd += ["-H", f"{key}: {value}"]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
    except FileNotFoundError:
        raise RuntimeError("curl not found on PATH - curl ships with Windows 10/11 by default.")

    if result.returncode != 0:
        print(f"curl exited with code {result.returncode}")
        print(f"curl stderr: {result.stderr!r}")

    output = result.stdout
    stripped = output.strip()
    if not stripped:
        raise RuntimeError("curl returned no output at all - check curl is installed and reachable.")

    *body_lines, status_code = stripped.rsplit("\n", 1)
    body = "\n".join(body_lines) if body_lines else ""
    return int(status_code), body


def get_authenticated_ws_url(app_id: str, api_token: str, account_type: str = "real") -> str:
    """
    Runs the REST auth flow and returns a ready-to-use authenticated
    WebSocket URL with the OTP already embedded.
    """
    headers = {
        "Deriv-App-ID": app_id,
        "Authorization": f"Bearer {api_token}",
    }

    # Step 1: list accounts tied to this token
    status, body = curl_request("GET", f"{REST_BASE}/trading/v1/options/accounts", headers)
    if status != 200:
        print(f"REST error {status}: {body}")
        raise RuntimeError(f"Failed to fetch accounts (status {status})")

    try:
        accounts = json.loads(body).get("data", [])
    except json.JSONDecodeError:
        raise RuntimeError(f"Accounts response wasn't valid JSON: {body!r}")

    if not accounts:
        raise RuntimeError("No accounts returned for this token - check your API token/app_id.")

    # Pick the first account matching the requested type (real/demo), else just the first one
    account = next(
        (a for a in accounts if a.get("account_type", "").lower() == account_type.lower()),
        accounts[0]
    )
    account_id = account.get("account_id")
    if not account_id:
        raise RuntimeError(f"Account data missing 'account_id': {account}")
    
    print(f"  Using account: {account_id} ({account.get('account_type', 'unknown')})")

    # Step 2: request an OTP for that account -> returns the full WS URL
    status, body = curl_request(
        "POST", f"{REST_BASE}/trading/v1/options/accounts/{account_id}/otp", headers
    )
    if status != 200:
        print(f"REST error {status}: {body}")
        raise RuntimeError(f"Failed to generate OTP (status {status})")

    try:
        ws_url = json.loads(body)["data"]["url"]
    except (json.JSONDecodeError, KeyError):
        raise RuntimeError(f"OTP response missing expected data: {body!r}")
    
    return ws_url


def build_digit_diff_trade(symbol: str, stake: float, digit: str) -> dict:
    return {
        "buy": "1",
        "price": stake,
        "parameters": {
            "contract_type": "DIGITDIFF",
            "currency": "USD",
            "underlying_symbol": symbol,
            "amount": stake,
            "basis": "stake",
            "duration": 1,
            "duration_unit": "t",
            "barrier": digit,
        },
    }


def load_config():
    try:
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return None

def save_config(asset, decimals, app_id, api_token, account_type, stake, max_trades):
    config = {
        "asset": asset,
        "decimals": decimals,
        "app_id": app_id,
        "api_token": api_token,
        "account_type": account_type,
        "stake": stake,
        "max_trades": max_trades,
        "last_updated": datetime.now().isoformat()
    }
    try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=2)
    except Exception as e:
        print(f"  Could not save config: {e}")

# ============= UI DISPLAY =============
class Display:
    @staticmethod
    def clear_screen():
        os.system('cls' if os.name == 'nt' else 'clear')

    @staticmethod
    def format_number(num: int) -> str:
        return f"{num:,}"

    @staticmethod
    def format_price(price: float, decimals: int) -> str:
        return f"{price:.{decimals}f}"

# ============= STREAK FOLLOW ENGINE WITH TRADE MANAGER =============
class StreakFollowEngine:
    def __init__(self, decimal_places: int, stake: float = 1.0, symbol: str = "R_100", websocket=None, max_trades: int = 0):
        self.decimal_places = decimal_places
        self.stake = stake
        self.symbol = symbol
        self.websocket = websocket
        self.max_trades = max_trades
        self.total_ticks = 0
        self.current_price = None
        self.last_price = None
        self.start_time = datetime.now()
        self.last_tick_time = datetime.now()
        self.recent_prices: List[Dict] = []
        self.is_loaded = False
        
        # Live tick feed
        self.tick_feed: List[Dict] = []
        self.max_feed_size = 20
        
        # ===== STATE MACHINE =====
        self.state = "IDLE"
        self.streak_digit = None
        self.streak_length = 0
        self.breakout_digit = None
        self.focus_digit = None
        self.chain_history: List[Dict] = []
        self.awaiting_next_digit = False
        
        # ===== TRADE MANAGER =====
        self.is_locked = True  # Start locked (paper trading only)
        self.paper_trade_count = 0
        self.real_trade_count = 0
        self.max_real_trades = max_trades
        
        # Paper trading stats
        self.paper_wins = 0
        self.paper_losses = 0
        self.paper_total_pnl = 0.0
        self.paper_loss_streak = 0
        
        # Real trading stats
        self.real_wins = 0
        self.real_losses = 0
        self.real_total_pnl = 0.0
        
        # Combined stats
        self.total_trades = 0
        self.total_wins = 0
        self.total_losses = 0
        self.total_pnl = 0.0
        
        # Trade history
        self.trade_history: List[Dict] = []
        self.pending_trade = None
        self.is_stopped = False
        
        # Track last digit for win/loss detection
        self.last_digit = None
        self.trade_result_pending = False
        
        # Speed tracking
        self.ticks_per_second = 0
        self.second_counter = 0
        self.last_update_time = datetime.now()

    def _get_last_digit(self, price: float) -> str:
        price_str = f"{price:.{self.decimal_places}f}".strip()
        if price_str.startswith('-'):
            price_str = price_str[1:]
        if '.' not in price_str:
            price_str = price_str + '.' + '0' * self.decimal_places
        integer_part, decimal_part = price_str.split('.')
        decimal_part = decimal_part.ljust(self.decimal_places, '0')
        if decimal_part:
            return decimal_part[-1]
        if integer_part:
            return integer_part[-1]
        return '0'

    def _determine_win_loss(self, digit: str) -> bool:
        """Win if next digit != breakout digit, loss if next digit == breakout digit"""
        return digit != self.breakout_digit

    def _place_real_trade(self) -> bool:
        if not self.websocket:
            print("  No websocket available for real trading")
            return False
        
        try:
            trade_request = build_digit_diff_trade(self.symbol, self.stake, self.breakout_digit)
            asyncio.create_task(self._send_trade(trade_request))
            return True
        except Exception as e:
            print(f"  Error placing real trade: {e}")
            return False
    
    async def _send_trade(self, trade_request: dict):
        try:
            if self.websocket:
                await self.websocket.send(json.dumps(trade_request))
                print(f"  Trade request sent: Digit Differs {self.breakout_digit} (${self.stake})")
        except Exception as e:
            print(f"  Failed to send trade: {e}")

    def process_trade_response(self, data: dict):
        if data.get("msg_type") == "buy":
            buy = data.get("buy")
            if buy:
                print(f"  Trade response received (waiting for next tick to determine result)")
        elif data.get("error"):
            error = data.get("error")
            error_msg = error.get("message", "Unknown error")
            print(f"  Trade error: {error_msg}")

    def _record_paper_trade(self, is_win: bool):
        """Record a paper trade result"""
        self.paper_trade_count += 1
        self.total_trades += 1
        
        if is_win:
            profit = self.stake * 0.85
            self.paper_wins += 1
            self.paper_total_pnl += profit
            self.paper_loss_streak = 0
            print(f"  {GREEN}PAPER WIN! +${profit:.2f}{RESET}")
        else:
            loss = -self.stake
            self.paper_losses += 1
            self.paper_total_pnl += loss
            self.paper_loss_streak += 1
            print(f"  {RED}PAPER LOSS! ${loss:.2f}{RESET}")
        
        self.total_wins = self.paper_wins + self.real_wins
        self.total_losses = self.paper_losses + self.real_losses
        self.total_pnl = self.paper_total_pnl + self.real_total_pnl
        
        self.trade_history.append({
            "time": datetime.now().strftime("%H:%M:%S.%f")[:-3],
            "type": "PAPER",
            "breakout_digit": self.breakout_digit,
            "stake": self.stake,
            "result": "WIN" if is_win else "LOSS",
            "pnl": profit if is_win else loss,
            "loss_streak": self.paper_loss_streak
        })
        
        # Check if we hit double loss (2 losses in a row)
        if self.is_locked and self.paper_loss_streak >= 2:
            self.is_locked = False
            print(f"\n  {YELLOW}DOUBLE LOSS DETECTED! ({self.paper_loss_streak} losses in a row){RESET}")
            print(f"  {GREEN}UNLOCKED! Waiting for next valid setup to place REAL trade{RESET}\n")
            self.paper_loss_streak = 0

    def _record_real_trade(self, is_win: bool):
        """Record a real trade result"""
        self.real_trade_count += 1
        self.total_trades += 1
        
        if is_win:
            profit = self.stake * 0.85
            self.real_wins += 1
            self.real_total_pnl += profit
            print(f"  {GREEN}REAL WIN! +${profit:.2f}{RESET}")
        else:
            loss = -self.stake
            self.real_losses += 1
            self.real_total_pnl += loss
            print(f"  {RED}REAL LOSS! ${loss:.2f}{RESET}")
        
        self.total_wins = self.paper_wins + self.real_wins
        self.total_losses = self.paper_losses + self.real_losses
        self.total_pnl = self.paper_total_pnl + self.real_total_pnl
        
        self.trade_history.append({
            "time": datetime.now().strftime("%H:%M:%S.%f")[:-3],
            "type": "REAL",
            "breakout_digit": self.breakout_digit,
            "stake": self.stake,
            "result": "WIN" if is_win else "LOSS",
            "pnl": profit if is_win else loss,
            "loss_streak": 0
        })
        
        # Lock back immediately after the real trade
        self.is_locked = True
        self.paper_loss_streak = 0
        print(f"  {YELLOW}LOCKED BACK. Resuming paper trading.{RESET}\n")
        
        # Check if max trades reached
        if self.max_real_trades > 0 and self.real_trade_count >= self.max_real_trades:
            self.is_stopped = True
            print(f"\n  {RED}MAX REAL TRADES REACHED! ({self.max_real_trades}) - STOPPING BOT{RESET}")

    def process_tick(self, price: float):
        if self.is_stopped:
            return
        
        self.total_ticks += 1
        self.second_counter += 1
        
        self.last_price = self.current_price
        self.current_price = price
        
        digit = self._get_last_digit(price)
        now_str = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        current_time = datetime.now()
        
        if (current_time - self.last_update_time).total_seconds() >= 1.0:
            self.ticks_per_second = self.second_counter
            self.second_counter = 0
            self.last_update_time = current_time
        
        # ===== STATE MACHINE =====
        if self.state == "IDLE":
            if self.streak_digit is None:
                self.streak_digit = digit
                self.streak_length = 1
            elif digit == self.streak_digit:
                self.streak_length += 1
            else:
                if self.streak_length >= 2:
                    self.breakout_digit = digit
                    self.focus_digit = digit
                    self.state = "BREAKOUT"
                    self.chain_history.append({
                        "time": now_str,
                        "streak_digit": self.streak_digit,
                        "streak_length": self.streak_length,
                        "breakout_digit": digit,
                        "type": "INITIAL"
                    })
                    print(f"  {YELLOW}Streak: {self.streak_digit}x{self.streak_length} -> Broken by {digit}{RESET}")
                else:
                    self.streak_digit = digit
                    self.streak_length = 1
        
        elif self.state == "BREAKOUT":
            if digit == self.breakout_digit:
                self.state = "FOCUS"
                self.chain_history.append({
                    "time": now_str,
                    "type": "STREAK",
                    "streak_digit": self.breakout_digit,
                    "streak_length": 2,
                    "message": f"{self.breakout_digit} is streaking, focusing on it"
                })
                print(f"  {CYAN}{self.breakout_digit} streaked! Now focusing on it{RESET}")
            else:
                if self.breakout_digit is not None and digit != self.breakout_digit:
                    print(f"  {GREEN}SETUP READY: Breakout={self.breakout_digit}, Next={digit} -> {digit} != {self.breakout_digit}{RESET}")
                    
                    # Check if locked or unlocked
                    if self.is_locked:
                        # PAPER TRADE
                        print(f"  {YELLOW}PAPER TRADE (LOCKED MODE){RESET}")
                        self.state = "TRADE_PLACED"
                        self.pending_trade = {
                            "type": "PAPER",
                            "breakout_digit": self.breakout_digit,
                            "stake": self.stake,
                            "tick": self.total_ticks
                        }
                    else:
                        # REAL TRADE (UNLOCKED)
                        print(f"  {GREEN}REAL TRADE (UNLOCKED!){RESET}")
                        self._place_real_trade()
                        self.state = "TRADE_PLACED"
                        self.pending_trade = {
                            "type": "REAL",
                            "breakout_digit": self.breakout_digit,
                            "stake": self.stake,
                            "tick": self.total_ticks
                        }
                    
                    self.awaiting_next_digit = False
                else:
                    self.state = "IDLE"
                    self.streak_digit = digit
                    self.streak_length = 1
        
        elif self.state == "FOCUS":
            if digit == self.focus_digit:
                self.chain_history.append({
                    "time": now_str,
                    "type": "STREAK",
                    "streak_digit": self.focus_digit,
                    "message": f"{self.focus_digit} streak continues"
                })
            else:
                old_focus = self.focus_digit
                self.breakout_digit = digit
                self.focus_digit = digit
                self.chain_history.append({
                    "time": now_str,
                    "type": "BREAKOUT",
                    "streak_digit": old_focus,
                    "breakout_digit": digit,
                    "message": f"{old_focus} broke by {digit} -> Now focusing on {digit}"
                })
                print(f"  {YELLOW}{old_focus} broke by {digit} -> Now focusing on {digit}{RESET}")
                self.state = "BREAKOUT"
        
        elif self.state == "TRADE_PLACED":
            if self.pending_trade:
                is_win = self._determine_win_loss(digit)
                
                if self.pending_trade["type"] == "PAPER":
                    self._record_paper_trade(is_win)
                else:
                    self._record_real_trade(is_win)
                
                self.pending_trade = None
                self.state = "IDLE"
                self.streak_digit = digit
                self.streak_length = 1
        
        # ===== UPDATE DISPLAY =====
        self.last_tick_time = current_time
        self.last_digit = digit
        
        self.recent_prices.append({
            "tick": self.total_ticks,
            "price": price,
            "digit": digit,
            "time": now_str
        })
        if len(self.recent_prices) > 10:
            self.recent_prices.pop(0)
        
        self.tick_feed.append({
            "tick": self.total_ticks,
            "price": price,
            "digit": digit,
            "time": now_str,
            "change": price - self.last_price if self.last_price else 0
        })
        if len(self.tick_feed) > self.max_feed_size:
            self.tick_feed.pop(0)
        
        self.is_loaded = True

    def get_state_display(self) -> str:
        if self.is_stopped:
            return f"{RED}STOPPED (Max trades: {self.max_real_trades} reached){RESET}"
        
        status = f"{'LOCKED' if self.is_locked else 'UNLOCKED'}"
        
        state_map = {
            "IDLE": f"IDLE (Waiting for streak...) [{status}]",
            "BREAKOUT": f"BREAKOUT: {self.breakout_digit} (Waiting for next tick) [{status}]",
            "FOCUS": f"FOCUS: {self.focus_digit} (Waiting for breakout) [{status}]",
            "TRADE_PLACED": "TRADE PLACED! (Waiting for result...)"
        }
        return state_map.get(self.state, self.state)

    def get_chain_summary(self) -> str:
        if not self.chain_history:
            return "No chain yet"
        
        parts = []
        for item in self.chain_history[-10:]:
            if item.get("type") == "INITIAL":
                parts.append(f"{item['streak_digit']}x{item['streak_length']}->{item['breakout_digit']}")
            elif item.get("type") == "BREAKOUT":
                parts.append(f"{item['streak_digit']}->{item['breakout_digit']}")
            elif item.get("type") == "STREAK":
                parts.append(f"{item['streak_digit']} streaking")
        
        return " | ".join(parts)

    def get_stats(self) -> Dict:
        paper_win_rate = (self.paper_wins / self.paper_trade_count * 100) if self.paper_trade_count > 0 else 0
        real_win_rate = (self.real_wins / self.real_trade_count * 100) if self.real_trade_count > 0 else 0
        total_win_rate = (self.total_wins / self.total_trades * 100) if self.total_trades > 0 else 0
        
        return {
            "total_ticks": self.total_ticks,
            "current_price": self.current_price,
            "last_price": self.last_price,
            "ticks_per_second": self.ticks_per_second,
            "recent_prices": self.recent_prices[-10:],
            "start_time": self.start_time,
            "time_since_last_tick": (datetime.now() - self.last_tick_time).total_seconds(),
            "last_tick_time": self.last_tick_time.strftime("%H:%M:%S.%f")[:-3],
            "is_loaded": self.is_loaded,
            "tick_feed": self.tick_feed[-15:],
            "state": self.state,
            "state_display": self.get_state_display(),
            "streak_digit": self.streak_digit,
            "streak_length": self.streak_length,
            "breakout_digit": self.breakout_digit,
            "focus_digit": self.focus_digit,
            "chain_summary": self.get_chain_summary(),
            "is_locked": self.is_locked,
            "is_stopped": self.is_stopped,
            "paper_trade_count": self.paper_trade_count,
            "real_trade_count": self.real_trade_count,
            "max_real_trades": self.max_real_trades,
            "paper_wins": self.paper_wins,
            "paper_losses": self.paper_losses,
            "paper_total_pnl": self.paper_total_pnl,
            "paper_win_rate": paper_win_rate,
            "paper_loss_streak": self.paper_loss_streak,
            "real_wins": self.real_wins,
            "real_losses": self.real_losses,
            "real_total_pnl": self.real_total_pnl,
            "real_win_rate": real_win_rate,
            "total_trades": self.total_trades,
            "total_wins": self.total_wins,
            "total_losses": self.total_losses,
            "total_pnl": self.total_pnl,
            "total_win_rate": total_win_rate,
            "trade_history": self.trade_history[-15:],
            "chain_history": self.chain_history,
        }

# ============= DERIV API (AUTHENTICATED STREAMING) =============
class DerivAuthenticatedAPI:
    def __init__(self, ws_url: str):
        self.ws_url = ws_url
        self.websocket = None
        self.connected = False

    async def connect(self):
        print(f"  Connecting to authenticated WebSocket...")
        try:
            self.websocket = await websockets.connect(
                self.ws_url,
                ping_interval=20,
                ping_timeout=10
            )
            self.connected = True
            print("  Connected to Deriv API (authenticated)")
            return True
        except Exception as e:
            print(f"  Connection failed: {e}")
            return False

    async def subscribe_ticks(self, symbol: str, callback):
        subscribe_msg = {
            "ticks": symbol,
            "subscribe": 1
        }
        
        print(f"  Subscribing to {symbol} live ticks...")
        await self.websocket.send(json.dumps(subscribe_msg))
        print(f"  Subscription request sent. Waiting for ticks...")
        
        while True:
            try:
                message = await self.websocket.recv()
                data = json.loads(message)
                
                msg_type = data.get("msg_type")
                
                if msg_type == "tick":
                    tick = data.get("tick")
                    if tick:
                        price = tick.get("quote")
                        if price is not None:
                            await callback(price=float(price))
                elif msg_type == "buy":
                    await callback(trade_response=data)
                elif data.get("error"):
                    error_msg = data.get("error", {}).get("message", "Unknown error")
                    print(f"  API Error: {error_msg}")
                    await callback(trade_error=data)
                    break
                    
            except websockets.exceptions.ConnectionClosed:
                print("  WebSocket connection closed")
                self.connected = False
                break
            except Exception as e:
                print(f"  Error in subscription: {e}")
                continue

    async def get_historical_ticks_rest(self, symbol: str, count: int) -> List[float]:
        url = f"{REST_BASE}/trading/v1/options/tick_history?underlying_symbol={symbol}&count={count}"
        try:
            status, body = curl_request("GET", url, {})
            if status == 200:
                data = json.loads(body)
                prices = data.get("data", [])
                return [float(p) for p in prices]
        except Exception as e:
            print(f"  Could not fetch historical ticks: {e}")
        return []

    async def close(self):
        self.connected = False
        if self.websocket:
            try:
                await self.websocket.close()
            except Exception:
                pass

# ============= MAIN BOT =============
class StreakFollowBot:
    def __init__(self):
        self.display = Display()
        self.api = None
        self.engine = None
        self.running = True
        self.asset = "R_100"
        self.decimals = 2
        self.stake = 1.0
        self.max_trades = 0
        self.app_id = ""
        self.api_token = ""
        self.account_type = "real"
        self.config_loaded = False

    def get_user_input(self):
        print("\n" + "=" * 60)
        print("  STREAK FOLLOW BOT WITH TRADE MANAGER")
        print("=" * 60)
        print("  Follows breakout chain until setup found")
        print("  Streak -> Breakout -> Focus -> Trade")
        print("  Paper trading until double loss unlocks REAL trades")
        print("=" * 60)

        saved_config = load_config()
        if saved_config:
            print("\n  Found saved configuration, loading automatically...")
            print(f"     Asset: {saved_config.get('asset', 'N/A')}")
            print(f"     Decimals: {saved_config.get('decimals', 'N/A')}")
            print(f"     Account Type: {saved_config.get('account_type', 'N/A')}")
            print(f"     Stake: ${saved_config.get('stake', 'N/A')}")
            print(f"     Max Trades: {saved_config.get('max_trades', 'N/A')}")
            
            self.asset = saved_config.get('asset', 'R_100')
            self.decimals = saved_config.get('decimals', 2)
            self.app_id = saved_config.get('app_id', '')
            self.api_token = saved_config.get('api_token', '')
            self.account_type = saved_config.get('account_type', 'real')
            self.stake = saved_config.get('stake', 1.0)
            self.max_trades = saved_config.get('max_trades', 0)
            self.config_loaded = True
            print("\n  Loaded saved configuration automatically")
            return

        print("\n  Common Deriv Symbols:")
        print("     Synthetic Indices: R_10, R_25, R_50, R_75, R_100")
        print("     Volatility Indices: 1HZ10V, 1HZ25V, 1HZ50V, 1HZ100V")
        print("     Forex: FRXEURUSD, FRXGBPUSD, FRXAUDUSD")
        print("     Crypto: BTCUSD, ETHUSD")
        print("")

        asset_input = input(f"  Enter asset symbol [R_100]: ").strip()
        self.asset = asset_input if asset_input else "R_100"

        if self.asset.startswith("R_") or self.asset.startswith("1HZ"):
            suggested_decimals = 2
            print(f"  Suggested decimals for {self.asset}: {suggested_decimals}")
            dec_input = input(f"  Enter decimal places [{suggested_decimals}]: ").strip()
            self.decimals = int(dec_input) if dec_input else suggested_decimals
        elif self.asset.upper().startswith("FRX"):
            suggested_decimals = 4
            print(f"  Suggested decimals for {self.asset}: {suggested_decimals}")
            dec_input = input(f"  Enter decimal places [{suggested_decimals}]: ").strip()
            self.decimals = int(dec_input) if dec_input else suggested_decimals
        elif self.asset in ["BTCUSD", "ETHUSD"]:
            suggested_decimals = 2
            print(f"  Suggested decimals for {self.asset}: {suggested_decimals}")
            dec_input = input(f"  Enter decimal places [{suggested_decimals}]: ").strip()
            self.decimals = int(dec_input) if dec_input else suggested_decimals
        else:
            while True:
                try:
                    dec_input = input("  Enter decimal places (2-5) [2]: ").strip()
                    if dec_input:
                        dec = int(dec_input)
                        if 2 <= dec <= 5:
                            self.decimals = dec
                            break
                        print("  Decimal places must be between 2 and 5")
                    else:
                        self.decimals = 2
                        break
                except ValueError:
                    print("  Please enter a valid number")

        print("\n  Trade Settings:")
        stake_input = input(f"  Stake per trade in USD [1.0]: ").strip()
        self.stake = float(stake_input) if stake_input else 1.0
        
        max_trades_input = input(f"  Max real trades (0 = unlimited) [0]: ").strip()
        self.max_trades = int(max_trades_input) if max_trades_input else 0

        print("\n  Authentication Settings:")
        self.app_id = input("  Enter your app_id: ").strip()
        self.api_token = input("  Enter your API token: ").strip()
        
        print("\n  Account Type:")
        print("  1. DEMO account")
        print("  2. REAL account")
        account_choice = input("  Select account type [1/2]: ").strip()
        self.account_type = "demo" if account_choice == "1" else "real"

        save_config(self.asset, self.decimals, self.app_id, self.api_token, self.account_type, self.stake, self.max_trades)
        print(f"\n  Configuration saved for next run")

        self.config_loaded = True

    async def initialize(self):
        self.get_user_input()

        print(f"\n  Configuration:")
        print(f"     Asset: {self.asset}")
        print(f"     Decimal Places: {self.decimals}")
        print(f"     Account Type: {self.account_type.upper()}")
        print(f"     Stake: ${self.stake}")
        print(f"     Max Real Trades: {self.max_trades if self.max_trades > 0 else 'Unlimited'}")
        print("=" * 60)

        print("\n  Authenticating with Deriv...")
        try:
            ws_url = get_authenticated_ws_url(self.app_id, self.api_token, self.account_type)
            print(f"  Authentication successful")
        except Exception as e:
            print(f"  Authentication failed: {e}")
            return False

        self.api = DerivAuthenticatedAPI(ws_url)

        print("\n  Connecting to Deriv...")
        if not await self.api.connect():
            print("\n  Failed to connect.")
            return False

        self.engine = StreakFollowEngine(self.decimals, self.stake, self.asset, self.api.websocket, self.max_trades)

        print(f"\n  Fetching initial historical ticks...")
        try:
            prices = await self.api.get_historical_ticks_rest(self.asset, 50)
            if prices:
                for price in prices:
                    self.engine.process_tick(price)
                print(f"  Loaded {len(prices)} historical ticks")
            else:
                print("  Could not fetch historical data")
        except Exception as e:
            print(f"  Could not fetch historical ticks: {e}")

        print("\n" + "=" * 60)
        print("  BOT STARTED - LOCKED MODE (PAPER TRADING)")
        print("  Waiting for 2 consecutive losses to unlock REAL trading")
        print("=" * 60)
        return True

    async def handle_live_tick(self, price: float = None, trade_response: dict = None, trade_error: dict = None):
        if trade_response:
            if self.engine:
                self.engine.process_trade_response(trade_response)
            return
        
        if trade_error:
            if self.engine:
                self.engine.process_trade_response(trade_error)
            return
        
        if price is not None and self.engine:
            self.engine.process_tick(price)

    def display_ui(self):
        self.display.clear_screen()
        stats = self.engine.get_stats()
        uptime = str(datetime.now() - stats['start_time']).split('.')[0]

        # Status display
        lock_status = "LOCKED (Paper Only)" if stats['is_locked'] else "UNLOCKED (Real Trading)"
        lock_color = RED if stats['is_locked'] else GREEN
        
        # Win/Loss displays
        paper_pnl_color = GREEN if stats['paper_total_pnl'] >= 0 else RED
        real_pnl_color = GREEN if stats['real_total_pnl'] >= 0 else RED
        total_pnl_color = GREEN if stats['total_pnl'] >= 0 else RED

        print("\n" + "=" * 100)
        
        if stats['is_stopped']:
            print(f"  {RED}BOT STOPPED - MAX REAL TRADES REACHED ({stats['max_real_trades']} trades){RESET}".center(100))
        else:
            print(f"  STREAK FOLLOW BOT WITH TRADE MANAGER".center(100))
        
        print("=" * 100)
        print(f"  Asset: {self.asset:<10} Decimals: {self.decimals}    Account: {self.account_type.upper()}")
        print(f"  Ticks: {self.display.format_number(stats['total_ticks']):>10}    "
              f"Speed: {stats['ticks_per_second']} ticks/sec    Uptime: {uptime}")
        print(f"  Status: {lock_color}{lock_status}{RESET}    Loss Streak: {stats['paper_loss_streak']}/2")
        print(f"  Real Trades: {stats['real_trade_count']}/{stats['max_real_trades'] if stats['max_real_trades'] > 0 else '∞'}")
        print("=" * 100)

        # ===== PAPER TRADING STATS =====
        print("\n  PAPER TRADING:")
        print(f"     Trades: {stats['paper_trade_count']} | Wins: {stats['paper_wins']} | Losses: {stats['paper_losses']}")
        print(f"     Win Rate: {stats['paper_win_rate']:.1f}% | P&L: {paper_pnl_color}${stats['paper_total_pnl']:.2f}{RESET}")

        # ===== REAL TRADING STATS =====
        print("\n  REAL TRADING:")
        print(f"     Trades: {stats['real_trade_count']} | Wins: {stats['real_wins']} | Losses: {stats['real_losses']}")
        print(f"     Win Rate: {stats['real_win_rate']:.1f}% | P&L: {real_pnl_color}${stats['real_total_pnl']:.2f}{RESET}")

        # ===== TOTAL STATS =====
        print("\n  TOTAL:")
        print(f"     Trades: {stats['total_trades']} | Wins: {stats['total_wins']} | Losses: {stats['total_losses']}")
        print(f"     Win Rate: {stats['total_win_rate']:.1f}% | P&L: {total_pnl_color}${stats['total_pnl']:.2f}{RESET}")

        # ===== STATE DISPLAY =====
        print("\n  State Machine:")
        print(f"     {stats['state_display']}")
        if stats['streak_digit'] and stats['state'] != "IDLE":
            print(f"     Current Streak: {stats['streak_digit']}x{stats['streak_length']}")
        if stats['breakout_digit']:
            print(f"     Breakout Digit: {stats['breakout_digit']}")
        if stats['focus_digit']:
            print(f"     Focus Digit: {stats['focus_digit']}")

        # ===== CHAIN HISTORY =====
        print("\n  Chain History:")
        if stats['chain_history']:
            print(f"     {stats['chain_summary']}")
        else:
            print("     No chain yet")

        # ===== RECENT TICKS =====
        print("\n  Recent Ticks:")
        print("  " + "-" * 50)
        print("  #    | PRICE        | DIGIT")
        print("  " + "-" * 50)

        recent = stats['recent_prices']
        for entry in recent:
            tick_str = f"#{entry['tick']:>4}"
            price_str = self.display.format_price(entry['price'], self.decimals)
            print(f"  {tick_str:>4} | {price_str:>12} |   {entry['digit']}")

        print("  " + "-" * 50)

        # ===== LIVE TICK FEED =====
        print("\n  Live Tick Feed:")
        print("  " + "-" * 45)
        print("  TIME     | TICK #   | PRICE")
        print("  " + "-" * 45)
        
        feed = stats['tick_feed']
        if feed:
            for entry in feed[-15:]:
                time_str = entry['time'][:8]
                tick_str = f"#{entry['tick']:>6}"
                price_str = self.display.format_price(entry['price'], self.decimals)
                print(f"  {time_str:>10} | {tick_str:>12} | {price_str:>12}")
        else:
            print("  Waiting for ticks...")
        
        print("  " + "-" * 45)
        print(f"  Showing last {min(len(feed), 15)} ticks received")

        # ===== TRADE HISTORY =====
        print("\n  Trade History:")
        if stats['trade_history']:
            print("  " + "-" * 75)
            print("  TIME     | TYPE    | BREAKOUT | RESULT | P&L")
            print("  " + "-" * 75)
            for trade in reversed(stats['trade_history'][-15:]):
                time_str = trade['time'][:8]
                trade_type = trade['type']
                breakout = trade['breakout_digit']
                result = trade['result']
                pnl = f"${trade['pnl']:.2f}"
                result_color = GREEN if result == "WIN" else RED
                pnl_color2 = GREEN if trade['pnl'] >= 0 else RED
                trade_type_color = GREEN if trade_type == "REAL" else YELLOW
                print(f"  {time_str:>10} | {trade_type_color}{trade_type:>7}{RESET} | {breakout:>7}    | {result_color}{result:>6}{RESET} | {pnl_color2}{pnl:>6}{RESET}")
            print("  " + "-" * 75)
        else:
            print("     No trades yet")

        # ===== SUMMARY =====
        print("\n  Summary:")
        print(f"     Status: {lock_color}{lock_status}{RESET}")
        print(f"     Paper Trades: {stats['paper_trade_count']} | Real Trades: {stats['real_trade_count']}")
        print(f"     Paper Win Rate: {stats['paper_win_rate']:.1f}% | Real Win Rate: {stats['real_win_rate']:.1f}%")
        print(f"     Total P&L: {total_pnl_color}${stats['total_pnl']:.2f}{RESET}")
        if stats['is_locked'] and stats['paper_loss_streak'] < 2:
            remaining = 2 - stats['paper_loss_streak']
            print(f"     Need {remaining} more loss(es) in a row to unlock REAL trading")
        elif stats['is_locked'] and stats['paper_loss_streak'] >= 2:
            print(f"     DOUBLE LOSS DETECTED! Waiting for next setup to place REAL trade")

        print("\n" + "=" * 100)
        print("  Press Ctrl+C to stop")

    async def run_live(self):
        print("\n  Starting live subscription...")
        
        subscribe_task = asyncio.create_task(
            self.api.subscribe_ticks(self.asset, self.handle_live_tick)
        )
        
        try:
            while self.running:
                self.display_ui()
                await asyncio.sleep(0.5)
                
                if self.engine and self.engine.is_stopped:
                    pass
                
        except KeyboardInterrupt:
            print("\n  Stopping bot...")
            self.running = False
        except Exception as e:
            print(f"\n  Error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            subscribe_task.cancel()
            try:
                await subscribe_task
            except asyncio.CancelledError:
                pass

# ============= MAIN ENTRY POINT =============
async def main():
    bot = StreakFollowBot()
    if not await bot.initialize():
        print("\n  Bot initialization failed. Exiting...")
        return
    await bot.run_live()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n  Bot stopped by user")
    except Exception as e:
        print(f"\n  Unexpected error: {e}")
        import traceback
        traceback.print_exc()
