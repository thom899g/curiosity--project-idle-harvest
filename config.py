"""
Configuration management for Project Idle Harvest.
Uses Pydantic for robust validation and type safety.
"""
import os
from typing import List, Optional
from dataclasses import dataclass
from decimal import Decimal
import logging
from pydantic import BaseModel, Field, validator
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class RPCConfig(BaseModel):
    """RPC endpoint configuration with validation"""
    quicknode_ws: str = Field(..., alias="QUICKNODE_WS_URL")
    alchemy_ws: str = Field(..., alias="ALCHEMY_WS_URL")
    infura_ws: str = Field(..., alias="INFURA_WS_URL")
    
    @validator('*')
    def validate_ws_url(cls, v):
        if not v.startswith(('ws://', 'wss://')):
            logger.warning(f"RPC URL doesn't start with ws:// or wss://: {v}")
        return v


class FirebaseConfig(BaseModel):
    """Firebase configuration with path validation"""
    credentials_path: str = Field(..., alias="FIREBASE_CREDENTIALS_PATH")
    project_id: str = Field(..., alias="FIREBASE_PROJECT_ID")
    
    @validator('credentials_path')
    def validate_credentials_path(cls, v):
        if not os.path.exists(v):
            raise FileNotFoundError(f"Firebase credentials file not found: {v}")
        return v


class SystemConfig(BaseModel):
    """System runtime configuration"""
    max_graph_nodes: int = Field(5000, alias="MAX_GRAPH_NODES")
    simulation_capital_levels: List[Decimal] = Field(default_factory=list)
    heat_threshold: float = Field(0.7, alias="HEAT_THRESHOLD", ge=0.0, le=1.0)
    gas_buffer_multiplier: float = Field(1.3, alias="GAS_BUFFER_MULTIPLIER", gt=1.0)
    
    @validator('simulation_capital_levels', pre=True)
    def parse_capital_levels(cls, v):
        if isinstance(v, str):
            return [Decimal(x.strip()) for x in v.split(',')]
        return v


class AlertConfig(BaseModel):
    """Alert system configuration"""
    telegram_bot_token: Optional[str] = Field(None, alias="TELEGRAM_BOT_TOKEN")
    telegram_chat_id: Optional[str] = Field(None, alias="TELEGRAM_CHAT_ID")


@dataclass
class ProjectConfig:
    """Main configuration container"""
    rpc: RPCConfig
    firebase: FirebaseConfig
    system: SystemConfig
    alerts: AlertConfig
    
    @classmethod
    def load(cls) -> 'ProjectConfig':
        """Load and validate all configuration"""
        try:
            return cls(
                rpc=RPCConfig(),
                firebase=FirebaseConfig(),
                system=SystemConfig(),
                alerts=AlertConfig()
            )
        except Exception as e:
            logger.error(f"Configuration loading failed: {e}")
            raise


# Global configuration instance
config = ProjectConfig.load()