"""
Main entry point for the Cluster Bus Fuzzer
"""
from .models import DSLConfig
from .fuzzer_engine import FuzzerEngine


class ClusterBusFuzzer:
    """Main orchestrator for the Cluster Bus Fuzzer system"""
    
    def __init__(self):
        """Initialize the fuzzer with the main fuzzer engine"""
        self.fuzzer_engine = FuzzerEngine()
    
    def run_random_test(self, seed: int = None):
        """
        Run a randomized test scenario.
        
        Args:
            seed: Optional seed for reproducibility
            
        Returns:
            ExecutionResult with test outcomes
        """
        scenario = self.fuzzer_engine.generate_random_scenario(seed)
        return self.fuzzer_engine.execute_test(scenario)
    
    def run_dsl_test(self, dsl_config: DSLConfig):
        """
        Run a DSL-based test scenario.
        
        Args:
            dsl_config: DSL configuration containing test specification
            
        Returns:
            ExecutionResult with test outcomes
        """
        return self.fuzzer_engine.execute_dsl_scenario(dsl_config)
    
    def validate_cluster(self, cluster_id: str):
        """
        Validate cluster state.
        
        Args:
            cluster_id: Cluster identifier
            
        Returns:
            ValidationResult with cluster state validation
        """
        return self.fuzzer_engine.validate_cluster_state(cluster_id)