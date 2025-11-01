import os
import time
import valkey
import subprocess
import shutil
import uuid
import logging
import traceback
from typing import List, Dict, Optional, Tuple
from ..models import ClusterConfig, NodePlan, NodeInfo, ClusterConnection

logging.basicConfig(format='%(levelname)-5s | %(filename)s:%(lineno)-3d | %(message)s', level=logging.INFO, force=True)

class PortManager:
    """Manages port allocation for Valkey cluster nodes"""
    
    def __init__(self, base_port: int = 6379, max_ports: int = 1000):
        self.base_port = base_port
        self.available_ports = set(range(base_port, base_port + max_ports))
        self.allocated_ports: Dict[str, Tuple[int, int]] = {}
    
    def allocate_ports(self, node_id: str) -> Tuple[int, int]:
        """Allocate client and bus ports for a node"""
        if not self.available_ports:
            raise Exception("No available ports!")
        
        client_port = min(self.available_ports)
        bus_port = client_port + 10000 
        
        self.available_ports.discard(client_port)
        self.allocated_ports[node_id] = (client_port, bus_port)
        
        logging.info(f"Allocated ports: {client_port}, {bus_port}")
        return client_port, bus_port
    
    def release_ports(self, node_id: str) -> None:
        """Release allocated ports for a node"""
        if node_id in self.allocated_ports:
            client_port, bus_port = self.allocated_ports[node_id]
            self.available_ports.add(client_port)
            del self.allocated_ports[node_id]
            logging.info(f"Released ports: {client_port}, {bus_port}")


class ConfigurationManager:
    """Manages Valkey cluster planning, node spawning, and resource cleanup"""

    def __init__(self, clusterConfig: ClusterConfig, port_manager: PortManager):
        self.clusterConfig = clusterConfig
        self.port_manager = port_manager
        self.cluster_id = self.generate_cluster_id()
    
    def setup_valkey_from_source(self, base_dir: str = "/tmp/valkey-build") -> str:
        """Clone and build Valkey binary, return path to valkey-server binary"""
        result = subprocess.run(['which', 'valkey-server'], capture_output=True, text=True)
        if result.returncode == 0:
            valkey_binary = result.stdout.strip()
            return valkey_binary
                
        valkey_dir = os.path.join(base_dir, "valkey")
        valkey_binary = os.path.join(valkey_dir, "src", "valkey-server")
       
        # Check if binary already exists from previous build
        if os.path.exists(valkey_binary):
            return valkey_binary
            
        os.makedirs(base_dir, exist_ok=True)
        
        subprocess.run(['git', 'clone', 'https://github.com/valkey-io/valkey.git', valkey_dir], check=True)
        subprocess.run(['make'], cwd=valkey_dir, check=True)
        
        if not os.path.exists(valkey_binary):
            raise Exception(f"Build completed but binary not found at {valkey_binary}")
        
        return valkey_binary
    
    def generate_cluster_id(self) -> str:
        """Generate unique cluster identifier"""
        return str(uuid.uuid4())[:8]
    
    def create_node_plan(self, node_counter: int, role: str, shard_id: int, 
                         slot_start: Optional[int] = None, slot_end: Optional[int] = None,
                         master_node_id: Optional[str] = None) -> NodePlan:
        """Create a node plan with allocated ports and configuration"""
        node_id = f"node-{node_counter}"
        client_port, bus_port = self.port_manager.allocate_ports(node_id)
        
        return NodePlan(
            node_id=node_id,
            role=role,
            shard_id=shard_id,
            port=client_port,
            bus_port=bus_port,
            slot_start=slot_start,
            slot_end=slot_end,
            master_node_id=master_node_id
        )
    
    def plan_topology(self) -> List[NodePlan]:
        """Plan cluster topology with given number of primary and replica nodes"""
        nodes = []
        node_counter = 0
        
        total_slots = 16384
        slots_per_shard = total_slots // self.clusterConfig.num_shards
        
        logging.info(f"Planning topology for {self.clusterConfig.num_shards} shards with {self.clusterConfig.replicas_per_shard} replica(s) each")
        print()
        
        for shard_num in range(self.clusterConfig.num_shards):
            slot_start = shard_num * slots_per_shard
            
            if shard_num == self.clusterConfig.num_shards - 1:
                slot_end = total_slots - 1
            else:
                slot_end = slot_start + slots_per_shard - 1
            
            primary_node_plan = self.create_node_plan(node_counter, 'primary', shard_num, slot_start, slot_end)
            nodes.append(primary_node_plan)
            logging.info(f"{primary_node_plan.node_id}: primary, shard {shard_num}, "
                        f"port {primary_node_plan.port}, slots {slot_start}-{slot_end}")
            node_counter += 1
            
            for _ in range(self.clusterConfig.replicas_per_shard):
                replica_plan = self.create_node_plan(node_counter, 'replica', shard_num, master_node_id=primary_node_plan.node_id)
                nodes.append(replica_plan)
                logging.info(f"{replica_plan.node_id}: replica, shard {shard_num}, "
                      f"port {replica_plan.port}, master={primary_node_plan.node_id}")
                node_counter += 1
        
        return nodes
    
    def create_node_directories(self, node_id: str) -> Tuple[str, str]:
        """Create directories for a node and return path"""
        node_data_dir = os.path.join(self.clusterConfig.base_data_dir, f"cluster-{self.cluster_id}", node_id, "data")
        log_dir = os.path.join(self.clusterConfig.base_data_dir, "logs")
        
        os.makedirs(node_data_dir, exist_ok=True)
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = os.path.join(log_dir, f"{node_id}.log")
        return node_data_dir, log_file
    
    def spawn_all_nodes(self, node_plans: List[NodePlan]) -> List[NodeInfo]:
        """Spawn all Valkey processes and wait for them to be ready"""
        print()
        logging.info(f"Spawning {len(node_plans)} nodes")
        node_info_list = []
        
        for plan in node_plans:
            node_data_dir, log_file = self.create_node_directories(plan.node_id)
            
            cmd = [
                self.clusterConfig.valkey_binary,
                '--port', str(plan.port),
                '--bind', '127.0.0.1',
                '--protected-mode', 'no',
                '--cluster-enabled', 'yes',
                '--cluster-config-file', os.path.join(node_data_dir, 'nodes.conf'),
                '--cluster-node-timeout', '5000',
                '--cluster-require-full-coverage', 'no',
                '--dir', node_data_dir,
                '--logfile', log_file,
                '--loglevel', 'notice',
                '--appendonly', 'yes',
                '--appendfilename', 'appendonly.aof',
                '--save', '',
                '--maxmemory', '500mb',
                '--maxmemory-policy', 'allkeys-lru'
            ]
            
            logging.info(f"Spawning {plan.node_id} on port {plan.port}")
            try:
                process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                
                node_info = NodeInfo(
                    node_id=plan.node_id,
                    role=plan.role,
                    shard_id=plan.shard_id,
                    port=plan.port,
                    bus_port=plan.bus_port,
                    pid=process.pid,
                    process=process,
                    data_dir=node_data_dir,
                    log_file=log_file,
                    slot_start=plan.slot_start,
                    slot_end=plan.slot_end,
                    master_node_id=plan.master_node_id
                )
                
                node_info_list.append(node_info)
                logging.info(f"Spawned {plan.node_id} with PID {process.pid}")
                
            except Exception as e:
                logging.info(f"Failed to spawn {plan.node_id}: {e}")
                for node in node_info_list:
                    self.terminate_node(node)
                raise
        
        logging.info(f"Waiting for all nodes to be ready")
        for node in node_info_list:
            deadline = time.time() + 30
            ready = False
            
            while time.time() < deadline:
                if node.process.poll() is not None:
                    logging.info(f"Node {node.node_id} process died")
                    break
                
                try:
                    client = valkey.Valkey(
                        host='127.0.0.1',
                        port=node.port,
                        socket_timeout=3,
                        decode_responses=True
                    )
                    
                    if client.ping():
                        logging.info(f"Node {node.node_id} is active")
                        ready = True
                        break
                except (valkey.ConnectionError, valkey.TimeoutError):
                    pass
                
                time.sleep(0.5)
            
            if not ready:
                for failed_node in node_info_list:
                    self.terminate_node(failed_node)
                raise Exception(f"Node {node.node_id} failed to start")
        
        logging.info(f"All {len(node_info_list)} nodes are active")
        return node_info_list
    
    def terminate_node(self, node: NodeInfo) -> None:
        """Terminate a Valkey node process"""
        if node.process.poll() is not None:
            return
        
        logging.info(f"Terminating {node.node_id} (PID {node.pid})")
        
        node.process.terminate()
        try:
            node.process.wait(timeout=5)
            logging.info(f"{node.node_id} terminated")
        except subprocess.TimeoutExpired:
            node.process.kill()
            node.process.wait()
            logging.info(f"{node.node_id} terminated")
    
    def cleanup_cluster(self, nodes_in_cluster: List[NodeInfo]) -> None:
        """Clean up cluster by terminating nodes and releasing resources"""
        print()
        logging.info(f"Cleaning up cluster {self.cluster_id}")
        
        for node in nodes_in_cluster:
            self.terminate_node(node)
        
        for node in nodes_in_cluster:
            self.port_manager.release_ports(node.node_id)
        
        if self.clusterConfig.enable_cleanup:
            cluster_dir = os.path.join(
                self.clusterConfig.base_data_dir,
                f"cluster-{self.cluster_id}"
            )
            if os.path.exists(cluster_dir):
                shutil.rmtree(cluster_dir)
                logging.info(f"Deleted data directory")
        
        logging.info(f"Cluster {self.cluster_id} cleaned up")


class ClusterManager:
    """Manages Valkey cluster formation and validates cluster health. Works with nodes that have been spawned by ConfigurationManager"""
    
    def __init__(self):
        self.connections: Dict[str, valkey.Valkey] = {}
    
    def get_client(self, node: NodeInfo) -> valkey.Valkey:
        """Get or create Valkey client connection for a node"""
        if node.node_id not in self.connections:
            self.connections[node.node_id] = valkey.Valkey(
                host='127.0.0.1',
                port=node.port,
                socket_timeout=5,
                decode_responses=True
            )
        return self.connections[node.node_id]
    
    def get_cluster_info(self, node: NodeInfo) -> Dict[str, str]:
        """Get cluster info from a node and parse into dictionary"""
        client = self.get_client(node)
        info = client.execute_command('CLUSTER', 'INFO')
        
        info_dict = {}
        for line in info.split('\r\n'):
            if ':' in line:
                key, value = line.split(':', 1)
                info_dict[key] = value
        return info_dict
    
    def get_node_role(self, node: NodeInfo) -> str:
        """Get the current role of a node (master/slave)"""
        client = self.get_client(node)
        nodes_info = client.execute_command('CLUSTER', 'NODES')
        
        # Parse CLUSTER NODES output to find this node's role
        for line in nodes_info.split('\n'):
            if 'myself' in line:
                if 'master' in line:
                    return 'master'
                elif 'slave' in line:
                    return 'slave'
        return 'unknown'
    
    def cluster_meet(self, nodes_in_cluster: List[NodeInfo], timeout: int = 30) -> None:
        """Connect cluster nodes and wait for convergence"""
        if len(nodes_in_cluster) < 2:
            return
        
        first_node = nodes_in_cluster[0]
        first_node_client = self.get_client(first_node)

        print()
        logging.info(f"Connecting {len(nodes_in_cluster)} nodes with CLUSTER MEET using {first_node.node_id} as starting node")
                
        for node in nodes_in_cluster[1:]:
            logging.info(f"Meeting {node.node_id} (port {node.port})")
            first_node_client.execute_command('CLUSTER', 'MEET', '127.0.0.1', node.port)
            time.sleep(0.1)
        
        logging.info("CLUSTER MEET complete")
        
        # Wait for convergence
        expected_count = len(nodes_in_cluster)
        deadline = time.time() + timeout
        
        logging.info(f"Waiting for cluster convergence for all {expected_count} nodes")
        
        while time.time() < deadline:
            all_converged = True
            
            for node in nodes_in_cluster:
                try:
                    info_dict = self.get_cluster_info(node)
                    known_nodes = int(info_dict.get('cluster_known_nodes', 0))
                    
                    if known_nodes < expected_count:
                        logging.info(f"{node.node_id} sees {known_nodes}/{expected_count} nodes")
                        all_converged = False
                    
                except Exception as e:
                    logging.info(f"Error checking {node.node_id}: {e}")
                    all_converged = False
            
            if all_converged:
                logging.info(f"All nodes see each other")
                return
            
            time.sleep(1)
        
        raise Exception(f"Cluster failed to converge within {timeout}s")
    
    def reset_cluster_state(self, nodes_in_cluster: List[NodeInfo]) -> None:
        """Reset cluster state on all nodes"""        
        logging.info("Resetting cluster state")
        for node in nodes_in_cluster:
            client = self.get_client(node)
            client.execute_command('CLUSTER', 'RESET', 'HARD')
        time.sleep(1)
    
    def assign_and_verify_slots(self, nodes_in_cluster: List[NodeInfo]) -> Dict[str, str]:
        """Assign hash slots to primary nodes and verify assignment"""
        primary_nodes = [node for node in nodes_in_cluster if node.role == 'primary']
        
        if not primary_nodes:
            logging.info("No primary nodes to assign slots to")
            return {}
        
        print()
        logging.info(f"Assigning slots to {len(primary_nodes)} primaries")
        
        node_ids = {}
        
        for primary in primary_nodes:
            client = self.get_client(primary)
            
            slots = list(range(primary.slot_start, primary.slot_end + 1))
            logging.info(f"Assigning slots {primary.slot_start}-{primary.slot_end} to {primary.node_id}")
            
            client.execute_command('CLUSTER', 'ADDSLOTS', *slots)
            
            # Get cluster node ID
            cluster_node_id = client.execute_command('CLUSTER', 'MYID')
            node_ids[primary.node_id] = cluster_node_id
            primary.cluster_node_id = cluster_node_id
            
            logging.info(f"{primary.node_id} cluster ID: {cluster_node_id[:8]}")
        
        logging.info("Slot assignment complete")
        time.sleep(5)
        
        # Verify slots were assigned correctly
        info_dict = self.get_cluster_info(nodes_in_cluster[0])
        slots_assigned = int(info_dict.get('cluster_slots_assigned', 0))
        slots_fail = int(info_dict.get('cluster_slots_fail', 0))
        
        logging.info(f"Slot verification:")
        logging.info(f"Slots assigned: {slots_assigned}/16384")
        logging.info(f"Slots failed: {slots_fail}")
        
        if slots_assigned != 16384 or slots_fail != 0:
            raise Exception(f"Slot assignment failed: {slots_assigned}/16384 assigned, {slots_fail} failed")
        
        return node_ids
    
    def setup_and_sync_replication(self, nodes_in_cluster: List[NodeInfo], primary_ids: Dict[str, str], timeout: int = 60) -> None:
        """Configure replication and wait for replicas to sync"""
        replicas = [node for node in nodes_in_cluster if node.role == 'replica']
        
        if not replicas:
            logging.info("No replicas to configure")
            return
        
        print()
        logging.info(f"Configuring {len(replicas)} replicas")
        
        for replica in replicas:
            master_cluster_id = primary_ids.get(replica.master_node_id)
            
            if not master_cluster_id:
                logging.info(f"Could not find master for {replica.node_id}")
                continue
            
            logging.info(f"Configuring {replica.node_id} to replicate {replica.master_node_id}")
            
            client = self.get_client(replica)
            client.execute_command('CLUSTER', 'REPLICATE', master_cluster_id)
            replica.cluster_node_id = client.execute_command('CLUSTER', 'MYID')
            
            logging.info(f"Replication configured")
        
        logging.info("Replication setup complete")
        
        logging.info(f"Waiting for {len(replicas)} replicas to sync")
        deadline = time.time() + timeout
        
        while time.time() < deadline:
            all_synced = True
            
            for replica in replicas:
                try:
                    client = self.get_client(replica)
                    info = client.info('replication')
                    
                    master_link = info.get('master_link_status')
                    
                    if master_link != 'up':
                        logging.info(f"  {replica.node_id}: {master_link}")
                        all_synced = False
                
                except Exception as e:
                    logging.info(f"Error checking {replica.node_id}: {e}")
                    all_synced = False
            
            if all_synced:
                logging.info("All replicas are in sync with their master")
                return
            
            time.sleep(1)
        
        raise Exception(f"Replicas failed to sync within {timeout}s")
    
    def validate_node_configs(self, nodes_in_cluster: List[NodeInfo], expected_configs: Dict[str, str]) -> bool:
        """Validate node configurations"""
        for node in nodes_in_cluster:
            client = self.get_client(node)
            config = client.config_get('*')
            
            for key, expected in expected_configs.items():
                actual = config.get(key, 'NOT_SET')
                if actual != expected:
                    logging.info(f"Config validation failed: {node.node_id} {key}, expected '{expected}', but got '{actual}'")
                    return False
        
        logging.info(f"All {len(nodes_in_cluster)} node configurations are correctly set")
        return True
    
    def validate_cluster(self, nodes_in_cluster: List[NodeInfo]) -> bool:
        if not nodes_in_cluster:
            return False
        
        print()
        logging.info("CLUSTER VALIDATION")
        
        # Collect cluster state from all nodes
        node_states = []
        unreachable_nodes = []
        
        for node in nodes_in_cluster:
            try:
                client = self.get_client(node)
                info_dict = self.get_cluster_info(node)
                
                cluster_state = info_dict.get('cluster_state')
                slots_assigned = int(info_dict.get('cluster_slots_assigned', 0))
                slots_fail = int(info_dict.get('cluster_slots_fail', 0))
                
                node_states.append({
                    'node_id': node.node_id,
                    'state': cluster_state,
                    'slots_assigned': slots_assigned,
                    'slots_fail': slots_fail
                })
                
                logging.info(f"{node.node_id}: state={cluster_state}, slots={slots_assigned}/16384, fail={slots_fail}")
                
            except Exception as e:
                unreachable_nodes.append(node.node_id)
                logging.warning(f"Expected node {node.node_id} is unreachable: {e}")
        
        # Fail if any expected nodes are unreachable
        if unreachable_nodes:
            logging.warning(f"Validation failed: {len(unreachable_nodes)} expected node(s) unreachable: {', '.join(unreachable_nodes)}")
            return False
        
        # Check if we got any responses (should always be true if no unreachable nodes)
        if not node_states:
            logging.warning(f"Could not reach any nodes for validation")
            return False
        
        # Check for consensus - all nodes should agree on cluster state
        first_state = node_states[0]
        consensus = all(
            state['state'] == first_state['state'] and
            state['slots_assigned'] == first_state['slots_assigned'] and
            state['slots_fail'] == first_state['slots_fail']
            for state in node_states
        )
        
        if not consensus:
            logging.warning("Validation failed: Cluster nodes have inconsistent views:")
            for state in node_states:
                logging.warning(f"  {state['node_id']}: {state['state']}, {state['slots_assigned']}/16384, fail={state['slots_fail']}")
            return False
        
        # Check if the consensus view is healthy
        is_healthy = (
            first_state['state'] == 'ok' and 
            first_state['slots_assigned'] == 16384 and 
            first_state['slots_fail'] == 0
        )
        
        if is_healthy:
            logging.info(f"Cluster is Healthy (all {len(node_states)} nodes reachable and consistent)")
        else:
            logging.info(f"Cluster is Not Healthy: state={first_state['state']}, slots={first_state['slots_assigned']}/16384")
        
        return is_healthy
    
    def form_cluster(self, nodes_in_cluster: List[NodeInfo]) -> ClusterConnection:
        """Form a complete cluster from spawned nodes"""
        print()
        logging.info("FORMING CLUSTER")
        
        try:
            self.reset_cluster_state(nodes_in_cluster)
            self.cluster_meet(nodes_in_cluster)
            
            primary_ids = self.assign_and_verify_slots(nodes_in_cluster)
                       
            self.setup_and_sync_replication(nodes_in_cluster, primary_ids)
            
            if not self.validate_cluster(nodes_in_cluster):
                raise Exception("Cluster validation failed")
            
            cluster_id = nodes_in_cluster[0].node_id.split('-')[0] if nodes_in_cluster else 'unknown'
            return ClusterConnection(nodes_in_cluster, cluster_id)
        
        except Exception as e:
            logging.info(f"Cluster formation failed: {e}")
            traceback.print_exc()
            return None
    
    def close_connections(self) -> None:
        """Close all Valkey client connections"""
        for client in self.connections.values():
            try:
                client.close()
            except:
                pass
        self.connections.clear()