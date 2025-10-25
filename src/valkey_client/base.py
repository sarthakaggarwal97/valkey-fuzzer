"""
Base classes for Valkey Client components
"""
from abc import ABC
from ..interfaces import IValkeyClient


class BaseValkeyClient(IValkeyClient, ABC):
    """Base implementation for Valkey client with common functionality"""
    
    def __init__(self):
        self.active_sessions = {}
        self.benchmark_binary_path = "valkey-benchmark"
        self.default_timeout = 30.0
    
    def _validate_cluster_connectivity(self, cluster_info) -> bool:
        """Validate that cluster is reachable for workload generation"""
        # Implementation will be added in later tasks
        pass
    
    def _generate_benchmark_command(self, cluster_info, workload_config) -> list:
        """Generate valkey-benchmark command arguments"""
        # Implementation will be added in later tasks
        pass