"""
Base classes for Fuzzer Engine components
"""
from abc import ABC
from ..interfaces import (
    IFuzzerEngine, ITestCaseGenerator, IOperationOrchestrator, 
    IStateValidator, ILogger
)


class BaseFuzzerEngine(IFuzzerEngine, ABC):
    """Base implementation for Fuzzer Engine with common functionality"""
    
    def __init__(self):
        self.test_case_generator = None
        self.operation_orchestrator = None
        self.state_validator = None
        self.logger = None
        self.cluster_orchestrator = None
        self.chaos_engine = None
        self.valkey_client = None


class BaseTestCaseGenerator(ITestCaseGenerator, ABC):
    """Base implementation for test case generation"""
    
    def __init__(self, random_seed: int = None):
        self.random_seed = random_seed


class BaseOperationOrchestrator(IOperationOrchestrator, ABC):
    """Base implementation for operation orchestration"""
    
    def __init__(self):
        self.active_operations = {}


class BaseStateValidator(IStateValidator, ABC):
    """Base implementation for state validation"""
    
    def __init__(self):
        self.validation_cache = {}


class BaseLogger(ILogger, ABC):
    """Base implementation for logging"""
    
    def __init__(self, log_file: str = None):
        self.log_file = log_file
        self.test_logs = []