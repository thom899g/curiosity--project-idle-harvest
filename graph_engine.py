"""
Stateful probabilistic graph engine for Base DEX ecosystem.
Maintains in-memory graph with predictive capabilities.
"""
import logging
from typing import Dict, List, Set, Tuple, Optional, Any
from collections import defaultdict
from datetime import datetime, timedelta
import hashlib

import networkx as nx
import pandas as pd
import numpy as np
from dataclasses import dataclass, field

from config import config

logger = logging.getLogger(__name__)


@dataclass
class PoolState:
    """State of a liquidity pool"""
    address: str
    token0: str
    token1: str
    reserve0: float = 0.0
    reserve1: float = 0.0
    fee_tier: int = 3000  # Default 0.3%
    last_update: datetime = field(default_factory=datetime.now)
    
    # Behavioral metrics
    volume_30s: float = 0.0
    price_30s_ma: float = 0.0
    lp_changes: List[Tuple[datetime, float]] = field(default_factory=list)  # (timestamp, delta)
    swap_sizes: List[float] = field(default_factory