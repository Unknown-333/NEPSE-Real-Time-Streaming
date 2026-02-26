"""
NEPSE Floorsheet Data Generator
=================================
PURPOSE:
    Generate realistic synthetic NEPSE floorsheet (transaction-by-transaction)
    data that mimics real market behavior. This data fuels our Kafka simulator.

WHY SYNTHETIC DATA instead of scraping live?
    1. Volume control: We need millions of rows. Real APIs rate-limit you.
    2. Reproducibility: Same seed → same dataset → deterministic testing.
    3. No IP bans: NEPSE's site blocks aggressive scraping.
    4. Time control: We can generate 5 years of data in minutes.

FLOORSHEET SCHEMA (matches real NEPSE floorsheet format):
    ┌──────────────┬──────────────────────────────────────────────┐
    │ Column       │ Description                                  │
    ├──────────────┼──────────────────────────────────────────────┤
    │ contract_no  │ Unique transaction ID (date-encoded)         │
    │ symbol       │ Stock ticker (e.g., NABIL, NICA)             │
    │ buyer        │ Buyer broker number (1-58)                   │
    │ seller       │ Seller broker number (1-58)                  │
    │ quantity     │ Number of shares traded                      │
    │ rate         │ Price per share (NPR)                        │
    │ amount       │ Total transaction value (quantity × rate)     │
    │ trade_time   │ Timestamp of the trade (ISO 8601)            │
    │ sector       │ Sector classification                        │
    └──────────────┴──────────────────────────────────────────────┘

REALISTIC BEHAVIORS MODELED:
    - Geometric Brownian Motion for price walks (how real stocks move)
    - Volume clustering: more trades near open/close (U-shaped curve)
    - Sector-weighted stock selection (banks dominate NEPSE volume)
    - Random quantity distributions matching real lot sizes
    - Different buyer/seller brokers per transaction
"""

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# ── Add project root to path for imports ──
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.data_generator.nepse_symbols import (
    BROKER_NUMBERS,
    NEPSE_STOCKS,
    SECTOR_WEIGHTS,
)
from src.utils.logger import get_logger

logger = get_logger("data_generator")


# ═══════════════════════════════════════════════════════════════
# Core Generator Class
# ═══════════════════════════════════════════════════════════════

class FloorsheetGenerator:
    """
    Generates realistic NEPSE floorsheet data.

    The generator models:
    1. Price movements via Geometric Brownian Motion (GBM)
    2. U-shaped intraday volume profile (high at open/close)
    3. Sector-proportional stock selection
    4. Realistic lot sizes and broker matching
    """

    def __init__(
        self,
        trading_days: int = 250,
        seed: int = 42,
        avg_trades_per_day: int = 50000,
    ):
        """
        Args:
            trading_days: Number of trading days to generate (~250 = 1 year).
            seed: Random seed for reproducibility.
            avg_trades_per_day: Average number of transactions per trading day.
                Real NEPSE generates ~50,000-100,000 floorsheet entries/day.
        """
        self.trading_days = trading_days
        self.avg_trades_per_day = avg_trades_per_day
        self.rng = np.random.default_rng(seed)

        # Build stock lookup with current (mutable) prices
        self.stocks = []
        for symbol, base_price, avg_vol, sector in NEPSE_STOCKS:
            self.stocks.append({
                "symbol": symbol,
                "current_price": base_price,
                "base_price": base_price,
                "avg_volume": avg_vol,
                "sector": sector,
            })

        # Pre-compute sector-weighted selection probabilities
        self._compute_stock_weights()

        logger.info(
            f"Generator initialized: {len(self.stocks)} stocks, "
            f"{trading_days} trading days, ~{avg_trades_per_day} trades/day"
        )

    def _compute_stock_weights(self) -> None:
        """
        Compute selection probability for each stock based on sector weights
        and individual volume profiles.

        WHY: In real NEPSE, ~40% of all trades are in commercial banks.
        A naive uniform random selection would be unrealistic.
        """
        weights = []
        for stock in self.stocks:
            sector = stock["sector"]
            sector_weight = SECTOR_WEIGHTS.get(sector, 0.02)
            # Within a sector, weight by relative volume
            weights.append(sector_weight * stock["avg_volume"])

        total = sum(weights)
        self.stock_weights = [w / total for w in weights]

    def _generate_trading_dates(self) -> list[datetime]:
        """
        Generate a list of trading dates (weekdays only, skip Sat/Sun).

        WHY: NEPSE trades Sunday-Thursday (Nepal's workweek), but for
        simplicity and international relevance, we use Mon-Fri.
        Nepal's Saturday is a holiday; Friday is a half-day in some periods.
        """
        # Start from ~5 years ago
        start_date = datetime(2021, 1, 3)  # A Monday
        dates = []
        current = start_date

        while len(dates) < self.trading_days:
            # Skip weekends (5=Saturday, 6=Sunday)
            if current.weekday() < 5:
                dates.append(current)
            current += timedelta(days=1)

        return dates

    def _intraday_volume_profile(self, num_trades: int) -> np.ndarray:
        """
        Generate U-shaped intraday trade distribution.

        WHY: Real stock markets show high volume at open and close,
        with a lull in the middle. This is called the "U-shape" or
        "smile" pattern. We model it with a transformed beta distribution.

        Returns:
            Array of floats in [0, 1] representing time-of-day fractions,
            where 0.0 = market open and 1.0 = market close.
        """
        # Beta(0.5, 0.5) produces a U-shaped distribution
        # More trades cluster near 0.0 (open) and 1.0 (close)
        time_fractions = self.rng.beta(0.7, 0.7, size=num_trades)
        return np.sort(time_fractions)

    def _walk_price(self, stock: dict, num_trades: int) -> np.ndarray:
        """
        Simulate intraday price movement using Geometric Brownian Motion.

        WHY: GBM is the standard model for stock price simulation.
        It ensures prices stay positive and exhibit realistic volatility.

        The formula: S(t+1) = S(t) * exp((mu - sigma²/2)*dt + sigma*sqrt(dt)*Z)
        where Z ~ N(0,1)

        Args:
            stock: Stock dict with 'current_price'.
            num_trades: Number of price points to generate.

        Returns:
            Array of simulated prices rounded to 2 decimal places.
        """
        price = stock["current_price"]

        # Daily drift (mu) and volatility (sigma) — annualized
        mu = 0.0002      # Slight upward drift (~5% annual)
        sigma = 0.015     # ~1.5% daily volatility (typical for NEPSE)
        dt = 1.0 / num_trades

        # Generate random returns
        z = self.rng.standard_normal(num_trades)
        log_returns = (mu - 0.5 * sigma**2) * dt + sigma * np.sqrt(dt) * z

        # Cumulative product to get price path
        prices = price * np.exp(np.cumsum(log_returns))

        # Ensure prices don't go below a floor (NEPSE has circuit breakers)
        floor = stock["base_price"] * 0.5
        ceiling = stock["base_price"] * 2.0
        prices = np.clip(prices, floor, ceiling)

        # Update the stock's current price to the last simulated price
        # (this carries over to the next trading day — creating multi-day trends)
        stock["current_price"] = prices[-1]

        return np.round(prices, 2)

    def _generate_contract_no(
        self, date: datetime, sequence: int
    ) -> str:
        """
        Generate a NEPSE-style contract number.

        Format: YYYYMMDD-SSSSSS  (date + 6-digit sequence)
        Real NEPSE encodes date + company code + sequence in a single ID.
        We simplify to date + sequence for clarity.
        """
        date_str = date.strftime("%Y%m%d")
        return f"{date_str}-{sequence:06d}"

    def generate_day(
        self, date: datetime, day_index: int
    ) -> pd.DataFrame:
        """
        Generate all floorsheet transactions for a single trading day.

        Args:
            date: The trading date.
            day_index: Index of this day (for progress logging).

        Returns:
            DataFrame with one row per transaction.
        """
        # Vary daily volume by ±20% (some days are busier)
        num_trades = int(
            self.avg_trades_per_day * self.rng.uniform(0.8, 1.2)
        )

        # Select stocks for each trade (weighted by sector/volume)
        stock_indices = self.rng.choice(
            len(self.stocks),
            size=num_trades,
            p=self.stock_weights,
        )

        # Generate intraday time distribution (U-shaped)
        time_fractions = self._intraday_volume_profile(num_trades)

        # Market hours: 11:00 to 15:00 (4 hours = 240 minutes)
        market_open = date.replace(hour=11, minute=0, second=0, microsecond=0)
        market_duration = timedelta(hours=4)

        records = []
        # Group trades by stock for efficient price walking
        stock_trade_counts = {}
        for idx in stock_indices:
            stock_trade_counts[idx] = stock_trade_counts.get(idx, 0) + 1

        # Pre-generate prices for each stock
        stock_prices = {}
        for stock_idx, count in stock_trade_counts.items():
            stock_prices[stock_idx] = self._walk_price(
                self.stocks[stock_idx], count
            )

        # Track price position per stock
        stock_price_pos = {idx: 0 for idx in stock_trade_counts}

        for i in range(num_trades):
            stock_idx = stock_indices[i]
            stock = self.stocks[stock_idx]

            # Get next price for this stock
            pos = stock_price_pos[stock_idx]
            rate = stock_prices[stock_idx][pos]
            stock_price_pos[stock_idx] = pos + 1

            # Generate realistic quantity (lot sizes: 10, 50, 100, 500, etc.)
            quantity = int(
                self.rng.choice([10, 20, 50, 100, 200, 500, 1000])
                * self.rng.uniform(0.5, 3.0)
            )
            quantity = max(10, quantity)  # Minimum lot size

            # Buyer and seller must be different brokers
            buyer = self.rng.choice(BROKER_NUMBERS)
            seller = self.rng.choice(
                [b for b in BROKER_NUMBERS if b != buyer]
            )

            # Calculate trade timestamp
            trade_time = market_open + market_duration * time_fractions[i]

            records.append({
                "contract_no": self._generate_contract_no(date, i + 1),
                "symbol": stock["symbol"],
                "buyer": int(buyer),
                "seller": int(seller),
                "quantity": int(quantity),
                "rate": float(rate),
                "amount": round(float(rate) * quantity, 2),
                "trade_time": trade_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "sector": stock["sector"],
            })

        df = pd.DataFrame(records)

        if (day_index + 1) % 50 == 0 or day_index == 0:
            logger.info(
                f"Day {day_index + 1}/{self.trading_days}: "
                f"{date.strftime('%Y-%m-%d')} → {len(df):,} trades generated"
            )

        return df

    def generate(self, output_dir: str | Path) -> Path:
        """
        Generate the full floorsheet dataset and save to CSV.

        Args:
            output_dir: Directory to save the output CSV.

        Returns:
            Path to the generated CSV file.
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        dates = self._generate_trading_dates()
        all_frames = []

        logger.info(
            f"Starting generation: {self.trading_days} days, "
            f"~{self.avg_trades_per_day:,} trades/day, "
            f"~{self.trading_days * self.avg_trades_per_day:,} total trades"
        )

        for i, date in enumerate(dates):
            day_df = self.generate_day(date, i)
            all_frames.append(day_df)

        # Concatenate all days
        full_df = pd.concat(all_frames, ignore_index=True)

        # Sort by trade_time globally
        full_df.sort_values("trade_time", inplace=True)
        full_df.reset_index(drop=True, inplace=True)

        # Save to CSV
        output_file = output_dir / "nepse_floorsheet.csv"
        full_df.to_csv(output_file, index=False)

        file_size_mb = output_file.stat().st_size / (1024 * 1024)

        logger.info(f"{'=' * 60}")
        logger.info(f"GENERATION COMPLETE")
        logger.info(f"{'=' * 60}")
        logger.info(f"  Output file : {output_file}")
        logger.info(f"  File size   : {file_size_mb:.1f} MB")
        logger.info(f"  Total rows  : {len(full_df):,}")
        logger.info(f"  Date range  : {dates[0].strftime('%Y-%m-%d')} → {dates[-1].strftime('%Y-%m-%d')}")
        logger.info(f"  Columns     : {list(full_df.columns)}")
        logger.info(f"{'=' * 60}")

        # Print sample
        logger.info("Sample rows (first 5):")
        print(full_df.head().to_string(index=False))

        return output_file


# ═══════════════════════════════════════════════════════════════
# CLI Entry Point
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic NEPSE floorsheet data for streaming simulation.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate 1 year (~250 days) with default settings
  python -m src.data_generator.generate_floorsheet

  # Generate 6 months with 30K trades/day (smaller dataset for testing)
  python -m src.data_generator.generate_floorsheet --days 125 --trades 30000

  # Generate 5 years of data (large dataset for production simulation)
  python -m src.data_generator.generate_floorsheet --days 1250 --trades 50000
        """,
    )
    parser.add_argument(
        "--days",
        type=int,
        default=250,
        help="Number of trading days to generate (default: 250 ≈ 1 year)",
    )
    parser.add_argument(
        "--trades",
        type=int,
        default=50000,
        help="Average trades per day (default: 50000)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output directory (default: data/raw/)",
    )

    args = parser.parse_args()

    # Resolve output directory
    if args.output:
        output_dir = Path(args.output)
    else:
        from src.config import DATA_DIR
        output_dir = DATA_DIR

    generator = FloorsheetGenerator(
        trading_days=args.days,
        seed=args.seed,
        avg_trades_per_day=args.trades,
    )

    generator.generate(output_dir)


if __name__ == "__main__":
    main()
