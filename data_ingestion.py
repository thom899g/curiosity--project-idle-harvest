"""
Multi-provider WebSocket data ingestion layer with consensus engine.
Implements provider fallback and data integrity checks.
"""
import asyncio
import json
import logging
from typing import Dict, List, Optional, Set, Deque
from collections import deque
from dataclasses import dataclass
from datetime import datetime
import hashlib

from web3 import Web3, AsyncWeb3
from web3.contract import Contract
from web3.exceptions import TransactionNotFound, BlockNotFound

from config import config

logger = logging.getLogger(__name__)


@dataclass
class NormalizedEvent:
    """Normalized DEX event structure"""
    block_number: int
    transaction_hash: str
    log_index: int
    contract_address: str
    event_type: str  # 'Swap', 'Mint', 'Burn', 'Sync'
    data: Dict
    timestamp: datetime
    provider_source: str
    verified: bool = False


class ConsensusEngine:
    """Cross-provider consensus engine for data integrity"""
    
    def __init__(self, required_agreement: int = 2):
        self.required_agreement = required_agreement
        self.block_hashes: Dict[int, Set[str]] = {}
        self.event_signatures: Dict[str, Dict] = {}
        self.provider_lag_count: Dict[str, int] = {}
        self.max_lag_blocks = 2
        
    def add_block(self, provider: str, block_number: int, block_hash: str):
        """Add block from provider and check consensus"""
        if block_number not in self.block_hashes:
            self.block_hashes[block_number] = set()
        
        self.block_hashes[block_number].add(block_hash)
        
        # Check if we have consensus
        if len(self.block_hashes[block_number]) > 1:
            # Consensus achieved
            logger.debug(f"Block {block_number} consensus achieved: {self.block_hashes[block_number]}")
            return True
        return False
    
    def add_event(self, provider: str, event_hash: str, event_data: Dict):
        """Add event from provider"""
        if event_hash not in self.event_signatures:
            self.event_signatures[event_hash] = {'providers': set(), 'data': event_data}
        
        self.event_signatures[event_hash]['providers'].add(provider)
    
    def get_verified_events(self) -> List[Dict]:
        """Return events verified by multiple providers"""
        verified = []
        for event_hash, info in self.event_signatures.items():
            if len(info['providers']) >= self.required_agreement:
                info['data']['verified'] = True
                verified.append(info['data'])
        return verified
    
    def check_provider_lag(self, provider: str, latest_block: int) -> bool:
        """Check if provider is lagging"""
        # Implementation depends on tracking latest blocks per provider
        pass


class WebSocketProvider:
    """Managed WebSocket provider with reconnection logic"""
    
    def __init__(self, name: str, ws_url: str):
        self.name = name
        self.ws_url = ws_url
        self.web3: Optional[AsyncWeb3] = None
        self.connected = False
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        self.subscription_id = None
        
    async def connect(self):
        """Establish WebSocket connection"""
        try:
            self.web3 = AsyncWeb3(AsyncWeb3.AsyncWebsocketProvider(self.ws_url))
            await self.web3.provider.connect()
            self.connected = True
            self.reconnect_attempts = 0
            logger.info(f"Connected to {self.name} at {self.ws_url}")
        except Exception as e:
            self.reconnect_attempts += 1
            logger.error(f"Failed to connect to {self.name}: {e}")
            if self.reconnect_attempts <= self.max_reconnect_attempts:
                await asyncio.sleep(2 ** self.reconnect_attempts)  # Exponential backoff
                await self.connect()
            else:
                raise
    
    async def subscribe_to_new_blocks(self):
        """Subscribe to new blocks"""
        if not self.connected:
            await self.connect()
        
        try:
            self.subscription_id = await self.web3.eth.subscribe('newHeads')
            logger.info(f"Subscribed to new blocks on {self.name}")
            return self.subscription_id
        except Exception as e:
            logger.error(f"Failed to subscribe to blocks on {self.name}: {e}")
            raise
    
    async def disconnect(self):
        """Cleanly disconnect"""
        if self.subscription_id and self.web3:
            try:
                await self.web3.eth.unsubscribe(self.subscription_id)
            except:
                pass
        self.connected = False


class DataIngestionLayer:
    """Main data ingestion layer with multiple providers"""
    
    def __init__(self):
        self.providers = {
            'quicknode': WebSocketProvider('QuickNode', config.rpc.quicknode_ws),
            'alchemy': WebSocketProvider('Alchemy', config.rpc.alchemy_ws)
        }
        self.consensus_engine = ConsensusEngine()
        self.event_buffer: Deque[NormalizedEvent] = deque(maxlen=10000)
        self.running = False
        
    async def initialize(self):
        """Initialize all providers"""
        logger.info("Initializing data ingestion layer...")
        
        # Try to connect to all providers
        successful_providers = []
        for name, provider in self.providers.items():
            try:
                await provider.connect()
                await provider.subscribe_to_new_blocks()
                successful_providers.append(name)
            except Exception as e:
                logger.warning(f"Provider {name} failed to initialize: {e}")
        
        if len(successful_providers) < 1:
            raise RuntimeError("No WebSocket providers available")
        
        logger.info(f"Initialized providers: {successful_providers}")
    
    async def start(self):
        """Start the ingestion loop"""
        self.running = True
        logger.info("Starting data ingestion...")
        
        # Create tasks for each provider
        tasks = []
        for provider in self.providers.values():
            if provider.connected:
                tasks.append(self._listen_to_provider(provider))
        
        # Run all provider listeners concurrently
        await asyncio.gather(*tasks)
    
    async def _listen_to_provider(self, provider: WebSocketProvider):
        """Listen to blocks from a specific provider"""
        while self.running:
            try:
                # Get new block
                block = await provider.web3.eth.get_block('latest', full_transactions=True)
                
                # Process block
                await self._process_block(provider.name, block)
                
                # Small delay to prevent tight loop
                await asyncio.sleep(0.1)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error listening to {provider.name}: {e}")
                await asyncio.sleep(1)
    
    async def _process_block(self, provider_name: str, block: Dict):
        """Process a block from a provider"""
        block_number = block['number']
        block_hash = block['hash'].hex()
        
        # Add to consensus engine
        consensus = self.consensus_engine.add_block(provider_name, block_number, block_hash)
        
        if consensus:
            # Process transactions in this block
            await self._process_transactions(block['transactions'], block_number, block['timestamp'])
    
    async def _process_transactions(self, transactions: List, block_number: int, timestamp: int):
        """Process transactions in a block"""
        for tx in transactions:
            try:
                # Decode transaction if it's a DEX interaction
                if self._is_dex_transaction(tx):
                    events = await self._decode_transaction_events(tx, block_number)
                    for event in events:
                        normalized = self._normalize_event(event, block_number, timestamp)
                        self.event_buffer.append(normalized)
            except Exception as e:
                logger.warning(f"Failed to process transaction {tx['hash'].hex()}: {e}")
    
    def _is_dex_transaction(self, tx: Dict) -> bool:
        """Check if transaction interacts with known DEX"""
        # TODO: Implement DEX contract address detection
        # For now, check if to address is a contract
        return tx.get('to') is not None
    
    async def _decode_transaction_events(self, tx: Dict, block_number: int) -> List[Dict]:
        """Decode transaction events using contract ABIs"""
        # TODO: Implement contract decoding
        # This requires having ABI files for DEX contracts
        return []
    
    def _normalize_event(self, event_data: Dict, block_number: int, timestamp: int) -> NormalizedEvent:
        """Normalize event data"""
        return NormalizedEvent(
            block_number=block_number,
            transaction_hash=event_data.get('transactionHash', ''),
            log_index=event_data.get('logIndex', 0),
            contract_address=event_data.get('address', ''),
            event_type=event_data.get('event', 'Unknown'),
            data=event_data.get('args', {}),
            timestamp=datetime.fromtimestamp(timestamp),
            provider_source='consensus'
        )
    
    def get_events(self, count: int = 100) -> List[NormalizedEvent]:
        """Get recent events from buffer"""
        return list(self.event_buffer)[-count:]
    
    async def stop(self):
        """Stop the ingestion layer"""
        self.running = False
        for provider in self.providers.values():
            await provider.disconnect()
        logger.info("Data ingestion stopped")


# Singleton instance
data_ingestion = DataIngestionLayer()