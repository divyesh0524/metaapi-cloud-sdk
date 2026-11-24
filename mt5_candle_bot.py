import asyncio
import os
from datetime import datetime, timezone
from metaapi_cloud_sdk import MetaApi

TOKEN = os.getenv("METAAPI_TOKEN", "")
ACCOUNT_ID = os.getenv("METAAPI_ACCOUNT_ID", "")
SYMBOL = os.getenv("SYMBOL", "XAUUSD")

START_LOT = 0.01
MAX_LOT = 0.10
TARGET_BALANCE = 40.0
DOUBLE_THRESHOLD = 4.0
EXIT_SECOND = 58


class CandleCompoundBot:
    def __init__(self, account):
        self.account = account
        self.current_lot = START_LOT
        self.step_profit = 0.0
        self.armed_to_double = False
        self.last_traded_bar = None
        self.last_seen_deal_id = None

    def normalize_lot(self, lot, min_lot=0.01, step_lot=0.01):
        lot = max(min_lot, min(MAX_LOT, lot))
        steps = int((lot - min_lot) / step_lot + 1e-9)
        return round(min_lot + max(0, steps) * step_lot, 2)

    async def sync_closed_deals(self):
        try:
            now = datetime.now(timezone.utc)
            start_time = datetime.fromtimestamp(now.timestamp() - 86400, tz=timezone.utc)
            history = await self.account.history_storage.get_deals_by_time_range(start_time, now)
            deals = history.get("deals", []) if isinstance(history, dict) else history
            if not deals:
                return

            for deal in deals:
                deal_id = str(deal.get("id", ""))
                if not deal_id or deal_id == self.last_seen_deal_id:
                    continue
                if deal.get("symbol") == SYMBOL and deal.get("entryType") == "DEAL_ENTRY_OUT":
                    pnl = (
                        float(deal.get("profit", 0.0))
                        + float(deal.get("swap", 0.0))
                        + float(deal.get("commission", 0.0))
                    )
                    self.last_seen_deal_id = deal_id
                    if pnl >= DOUBLE_THRESHOLD:
                        self.armed_to_double = True
                        self.step_profit = 0.0
                    elif pnl > 0:
                        self.step_profit += pnl
                        if self.step_profit >= DOUBLE_THRESHOLD:
                            self.armed_to_double = True
                            self.step_profit = 0.0
                    else:
                        self.armed_to_double = False
                        self.step_profit = 0.0
        except Exception as e:
            print("Deal sync warning:", e)

    async def choose_lot(self, price_info):
        acc_info = await self.account.get_account_information()
        balance = float(acc_info.get("balance", 0.0))
        free_margin = float(acc_info.get("freeMargin", 0.0))

        lot = self.current_lot
        if balance >= TARGET_BALANCE and lot < MAX_LOT:
            lot = self.normalize_lot(lot * 2.0)

        if self.armed_to_double:
            lot = self.normalize_lot(lot * 2.0)
            self.armed_to_double = False
            self.step_profit = 0.0

        # Basic free-margin safety guard
        while lot > 0.01 and free_margin < (lot * 200.0):
            lot = self.normalize_lot(lot - 0.01)

        self.current_lot = lot
        return lot

    async def close_symbol_positions(self):
        positions = await self.account.get_positions()
        for pos in positions:
            if pos.get("symbol") == SYMBOL:
                pos_id = pos.get("id")
                await self.account.close_position(pos_id)
                print(f"Closed position {pos_id} at exit window.")

    async def run(self):
        await self.account.wait_synchronized()
        print(f"Bot connected & synchronized for {SYMBOL}...")

        while True:
            now = datetime.now(timezone.utc)
            sec = now.second
            bar_start = now.replace(second=0, microsecond=0)

            await self.sync_closed_deals()

            # Exit at 58th second before candle closes
            if sec >= EXIT_SECOND:
                positions = await self.account.get_positions()
                if any(p.get("symbol") == SYMBOL for p in positions):
                    await self.close_symbol_positions()
                    await asyncio.sleep(1)
                    await self.sync_closed_deals()

            # Enter on new 1-minute candle open (seconds 0..2)
            positions = await self.account.get_positions()
            has_open = any(p.get("symbol") == SYMBOL for p in positions)

            if sec <= 2 and not has_open and self.last_traded_bar != bar_start:
                candles = await self.account.get_candle(SYMBOL, "1m", 0)
                price = await self.account.get_price(SYMBOL)

                candle_open = float(candles.get("open", 0.0)) if candles else 0.0
                ask = float(price.get("ask", 0.0)) if price else 0.0
                bid = float(price.get("bid", 0.0)) if price else 0.0

                if candle_open > 0 and ask > 0 and bid > 0:
                    lot = await self.choose_lot(price)
                    if ask >= candle_open and candle_open < ask:
                        await self.account.create_market_buy_order(
                            SYMBOL, lot, stop_loss=candle_open, take_profit=None
                        )
                        self.last_traded_bar = bar_start
                        print(f"BUY {lot} {SYMBOL} @ {ask}, SL={candle_open}")
                    elif bid < candle_open and candle_open > bid:
                        await self.account.create_market_sell_order(
                            SYMBOL, lot, stop_loss=candle_open, take_profit=None
                        )
                        self.last_traded_bar = bar_start
                        print(f"SELL {lot} {SYMBOL} @ {bid}, SL={candle_open}")

            await asyncio.sleep(0.5)


async def main():
    if not TOKEN or not ACCOUNT_ID:
        raise ValueError("METAAPI_TOKEN and METAAPI_ACCOUNT_ID environment variables are required.")

    api = MetaApi(token=TOKEN)
    account = await api.metatrader_account_api.get_account(ACCOUNT_ID)

    if account.state != "DEPLOYED":
        await account.deploy()

    connection = account.get_streaming_connection()
    await connection.connect()

    bot = CandleCompoundBot(account)
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
                      
