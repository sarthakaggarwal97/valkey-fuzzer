"""
Base classes for Chaos Engine components
"""
from abc import ABC
from ..interfaces import IChaosEngine


class BaseChaosEngine(IChaosEngine, ABC):
    """Base implementation for chaos injection with common functionality"""
    
    def __init__(self):
        self.active_chaos = {}
        self.chaos_history = []
        self.coordination_enabled = True
    
    def _validate_chaos_target(self, target_node) -> bool:
        """Validate that chaos can be injected on target node"""
        # Implementation will be added in later tasks
        pass
    
    def _cleanup_process_chaos(self, chaos_id: str) -> bool:
        """Clean up process chaos effects"""
        # Implementation will be added in later tasks
        pass