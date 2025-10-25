"""
Base classes for Cluster Orchestrator components
"""
from abc import ABC
from ..interfaces import IClusterOrchestrator


class BaseClusterOrchestrator(IClusterOrchestrator, ABC):
    """Base implementation for cluster orchestration with common functionality"""
    
    def __init__(self):
        self.active_clusters = {}
        self.node_port_range = (7000, 8000)
        self.valkey_binary_path = "valkey-server"
        self.cluster_timeout = 60.0
    
    def _get_next_available_port(self) -> int:
        """Get next available port for node creation"""
        # Implementation will be added in later tasks
        pass
    
    def _validate_cluster_config(self, config) -> bool:
        """Validate cluster configuration before creation"""
        # Implementation will be added in later tasks
        pass