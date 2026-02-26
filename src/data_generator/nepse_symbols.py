"""
NEPSE Stock Symbols & Sector Reference Data
=============================================
WHY this file exists:
Real NEPSE floorsheet data contains stock symbols (like NABIL, NICA, NLIC)
and broker numbers. To generate realistic synthetic data, we need:
  1. Real stock symbols grouped by sector
  2. Realistic base prices per stock (to simulate price movements)
  3. Realistic volume profiles (banking stocks trade more than hydro)
  4. Real broker numbers (1-58 as of NEPSE's broker list)

This acts as our "seed data" — the DNA of realistic simulation.

Data sourced from: NEPSE official listings, NepseAlpha, MeroLagani
"""

# ═══════════════════════════════════════════════════════════════
# NEPSE Stock Universe — Top stocks by sector
# Format: (Symbol, Base Price NPR, Avg Daily Volume, Sector)
# ═══════════════════════════════════════════════════════════════

NEPSE_STOCKS = [
    # ── Commercial Banks (highest volume, ~40% of market) ──
    ("NABIL", 1150.0, 25000, "Commercial Bank"),
    ("NICA", 820.0, 30000, "Commercial Bank"),
    ("SBL", 310.0, 45000, "Commercial Bank"),
    ("KBL", 280.0, 35000, "Commercial Bank"),
    ("GBIME", 320.0, 40000, "Commercial Bank"),
    ("SANIMA", 380.0, 28000, "Commercial Bank"),
    ("MEGA", 295.0, 32000, "Commercial Bank"),
    ("CZBIL", 350.0, 22000, "Commercial Bank"),
    ("PRVU", 290.0, 27000, "Commercial Bank"),
    ("SCB", 620.0, 15000, "Commercial Bank"),
    ("HBL", 480.0, 20000, "Commercial Bank"),
    ("EBL", 540.0, 18000, "Commercial Bank"),
    ("ADBL", 410.0, 25000, "Commercial Bank"),
    ("NBL", 380.0, 22000, "Commercial Bank"),
    ("MBL", 310.0, 30000, "Commercial Bank"),

    # ── Development Banks ──
    ("MNBBL", 380.0, 12000, "Development Bank"),
    ("EDBL", 290.0, 10000, "Development Bank"),
    ("GBBL", 310.0, 8000, "Development Bank"),
    ("KSBBL", 350.0, 9000, "Development Bank"),
    ("LBBL", 270.0, 11000, "Development Bank"),
    ("MLBL", 260.0, 7000, "Development Bank"),
    ("SADBL", 320.0, 8500, "Development Bank"),
    ("SHINE", 340.0, 9500, "Development Bank"),

    # ── Life Insurance ──
    ("NLIC", 780.0, 15000, "Life Insurance"),
    ("ALICL", 680.0, 12000, "Life Insurance"),
    ("GLICL", 520.0, 10000, "Life Insurance"),
    ("PLIC", 610.0, 8000, "Life Insurance"),
    ("NLICL", 480.0, 9000, "Life Insurance"),
    ("SLICL", 550.0, 7500, "Life Insurance"),

    # ── Non-Life Insurance ──
    ("SICL", 1050.0, 5000, "Non-Life Insurance"),
    ("NICL", 880.0, 6000, "Non-Life Insurance"),
    ("HGI", 720.0, 4500, "Non-Life Insurance"),
    ("PRIN", 650.0, 5500, "Non-Life Insurance"),
    ("AIL", 590.0, 4000, "Non-Life Insurance"),

    # ── Hydropower ──
    ("NHPC", 620.0, 20000, "Hydropower"),
    ("BPCL", 480.0, 18000, "Hydropower"),
    ("AKPL", 350.0, 15000, "Hydropower"),
    ("RURU", 410.0, 12000, "Hydropower"),
    ("UPPER", 380.0, 25000, "Hydropower"),
    ("API", 520.0, 16000, "Hydropower"),
    ("GVL", 290.0, 22000, "Hydropower"),
    ("KPCL", 340.0, 10000, "Hydropower"),
    ("MBJC", 270.0, 8000, "Hydropower"),
    ("SPDL", 310.0, 9000, "Hydropower"),

    # ── Microfinance ──
    ("CBBL", 1650.0, 5000, "Microfinance"),
    ("DDBL", 1200.0, 4000, "Microfinance"),
    ("FOWAD", 980.0, 3500, "Microfinance"),
    ("GBLBS", 880.0, 3000, "Microfinance"),
    ("KLBSL", 1100.0, 4500, "Microfinance"),
    ("MLBSL", 950.0, 3200, "Microfinance"),
    ("NMBMF", 1050.0, 3800, "Microfinance"),
    ("SMFDB", 780.0, 2500, "Microfinance"),

    # ── Finance Companies ──
    ("GFCL", 250.0, 8000, "Finance"),
    ("GUFL", 280.0, 7000, "Finance"),
    ("ICFC", 310.0, 6500, "Finance"),
    ("JFL", 270.0, 5500, "Finance"),
    ("MFIL", 230.0, 6000, "Finance"),
    ("MPFL", 260.0, 5000, "Finance"),

    # ── Manufacturing & Others ──
    ("UNL", 3200.0, 2000, "Manufacturing"),
    ("BNT", 1800.0, 3000, "Manufacturing"),
    ("HDL", 2500.0, 2500, "Manufacturing"),
    ("SHIVM", 750.0, 8000, "Manufacturing"),

    # ── Hotels ──
    ("TRH", 290.0, 4000, "Hotel"),
    ("SHL", 480.0, 3500, "Hotel"),
    ("OHL", 520.0, 3000, "Hotel"),
]

# ═══════════════════════════════════════════════════════════════
# NEPSE Broker Numbers (1 through 58)
# ═══════════════════════════════════════════════════════════════
# In real floorsheets, buyers and sellers are identified by broker
# numbers, not names. This range covers all active NEPSE brokers.
BROKER_NUMBERS = list(range(1, 59))

# ═══════════════════════════════════════════════════════════════
# Sector Weights (for realistic sector-proportional generation)
# ═══════════════════════════════════════════════════════════════
SECTOR_WEIGHTS = {
    "Commercial Bank": 0.40,
    "Development Bank": 0.10,
    "Life Insurance": 0.10,
    "Non-Life Insurance": 0.05,
    "Hydropower": 0.15,
    "Microfinance": 0.08,
    "Finance": 0.05,
    "Manufacturing": 0.04,
    "Hotel": 0.03,
}
