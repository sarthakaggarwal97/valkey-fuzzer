"""
Main entry point for the Cluster Bus Fuzzer
"""
from .models import Scenario, ClusterConfig, DSLConfig
from .interfaces import IFuzzerEngine, IClusterOrchestrator, IChaosEngine, IValkeyClient
from .fuzzer_engine.base import BaseFuzzerEngine
from .cluster_orchestrator.base import BaseClusterOrchestrator
from .chaos_engine.base import BaseChaosEngine
from .valkey_client.base import BaseValkeyClient


class ClusterBusFuzzer:
    """Main orchestrator for the Cluster Bus Fuzzer system"""
    
    def __init__(self, 
                 fuzzer_engine: IFuzzerEngine = None,
                 cluster_orchestrator: IClusterOrchestrator = None,
                 chaos_engine: IChaosEngine = None,
                 valkey_client: IValkeyClient = None):
        """
        Initialize the fuzzer with component implementations
        
        Args:
            fuzzer_engine: Implementation of fuzzer engine interface
            cluster_orchestrator: Implementation of cluster orchestrator interface
            chaos_engine: Implementation of chaos engine interface
            valkey_client: Implementation of valkey client interface
        """
        self.fuzzer_engine = fuzzer_engine
        self.cluster_orchestrator = cluster_orchestrator
        self.chaos_engine = chaos_engine
        self.valkey_client = valkey_client
    
    def run_random_test(self, seed: int = None):
        """Run a randomized test scenario"""
        if not self.fuzzer_engine:
            raise RuntimeError("Fuzzer engine not initialized")
        
        scenario = self.fuzzer_engine.generate_random_scenario(seed)
        return self.fuzzer_engine.execute_test(scenario)
    
    def run_dsl_test(self, dsl_config: DSLConfig):
        """Run a DSL-based test scenario"""
        if not self.fuzzer_engine:
            raise RuntimeError("Fuzzer engine not initialized")
        
        return self.fuzzer_engine.execute_dsl_scenario(dsl_config)
    
    def validate_cluster(self, cluster_id: str):
        """Validate cluster state"""
        if not self.fuzzer_engine:
            raise RuntimeError("Fuzzer engine not initialized")
        
        return self.fuzzer_engine.validate_cluster_state(cluster_id)