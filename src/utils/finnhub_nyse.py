import time
from datetime import datetime, timedelta


def sleep_until_next_minute_plus_10():
    """
    Sleeps until 10 seconds past the next full minute.

    :return: None
    """
    now = datetime.now()
    next_minute = now + timedelta(minutes=1)
    next_minute = next_minute.replace(second=10, microsecond=0)
    sleep_time = (next_minute - now).total_seconds()
    time.sleep(sleep_time)


def is_within_work_hours():
    """
    Checks whether the current time is within work hours (Mon–Fri, 6:30–13:00).

    :return: True if current time is within work hours, else False
    """
    now = datetime.now()
    weekday = now.weekday()  # 0: Monday, 1: Tuesday, ..., 4: Friday
    hour = now.hour
    minute = now.minute

    return weekday < 5 and (7 <= hour <= 12 or (hour == 6 and minute >= 30) or (hour == 13 and minute == 0))


# Top overall stocks (volume)
OVERALL_TOP_10 = [
    'TSLA',  # Tesla
    'AAPL',  # Apple
    'NVDA',  # NVIDIA
    'MSFT',  # Microsoft
    'META',  # Meta Platforms (Facebook)
    'AMD',   # Advanced Micro Devices
    'NFLX',  # Netflix
    'GOOG',  # Alphabet (Google)
    'AMZN',  # Amazon
    'BABA',  # Alibaba Group
]
# Top healthcare stocks (volume)
HEALTH_TOP_20 = [
    'JNJ',   # Johnson & Johnson
    'ABBV',  # AbbVie
    'PFE',   # Pfizer
    'MRK',   # Merck & Co.
    'UNH',   # UnitedHealth Group
    'CVS',   # CVS Health
    'ZBH',   # Zimmer Biomet Holdings
    'BIIB',  # Biogen
    'TMO',   # Thermo Fisher Scientific
    'AMGN',  # Amgen
    'BMY',   # Bristol Myers Squibb
    'MDT',   # Medtronic
    'REGN',  # Regeneron Pharmaceuticals
    'ISRG',  # Intuitive Surgical
    'LLY',   # Eli Lilly and Company
]
# Top energy stocks (volume)
ENERGY_TOP_20 = [
    "XOM",   # ExxonMobil
    "CVX",   # Chevron
    "COP",   # ConocoPhillips
    "NEE",   # NextEra Energy
    "MPC",   # Marathon Petroleum
    "OXY",   # Occidental Petroleum
    "DUK",   # Duke Energy
    "WMB",   # Williams Companies
    "VLO",   # Valero Energy
    "SO",    # Southern Company
    "APA",   # APA Corporation
    "DVN",   # Devon Energy
    "EOG",   # EOG Resources
    "ED",    # Consolidated Edison
    "NRG",   # NRG Energy
]

TOP_STOCKS = {'Top Overall': OVERALL_TOP_10, 'Health': HEALTH_TOP_20, 'Energy': ENERGY_TOP_20}
