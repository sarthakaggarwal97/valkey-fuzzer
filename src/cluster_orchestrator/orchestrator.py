import os
import time
import valkey
import subprocess
import shutil
import uuid
from typing import List, Dict, Optional, Tuple
from ..models import ClusterConfig, NodePlan, NodeInfo


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
        
        return client_port, bus_port
    
    def release_ports(self, node_id: str) -> None:
        """Release allocated ports for a node"""
        if node_id in self.allocated_ports:
            client_port, bus_port = self.allocated_ports[node_id]
            self.available_ports.add(client_port)
            del self.allocated_ports[node_id]
            print(f"Released ports: {client_port}, {bus_port}")


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
        os.makedirs(base_dir, exist_ok=True)
        
        subprocess.run(['git', 'clone', 'https://github.com/valkey-io/valkey.git', valkey_dir], check=True)
        subprocess.run(['make'], cwd=valkey_dir, check=True)
        
        if not os.path.exists(valkey_binary):
            raise Exception(f"Build completed but binary not found at {valkey_binary}")
        
        print(f"Valkey built successfully at: {valkey_binary}")
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
        
        print(f"\nPlanning topology for {self.clusterConfig.num_shards} shards with "
              f"{self.clusterConfig.replicas_per_shard} replica(s) each")
        
        for shard_num in range(self.clusterConfig.num_shards):
            slot_start = shard_num * slots_per_shard
            
            if shard_num == self.clusterConfig.num_shards - 1:
                slot_end = total_slots - 1
            else:
                slot_end = slot_start + slots_per_shard - 1
            
            primary_node_plan = self.create_node_plan(node_counter, 'primary', shard_num, slot_start, slot_end)
            nodes.append(primary_node_plan)
            print(f"{primary_node_plan.node_id}: primary, shard {shard_num}, "
                  f"port {primary_node_plan.port}, slots {slot_start}-{slot_end}")
            node_counter += 1
            
            for _ in range(self.clusterConfig.replicas_per_shard):
                replica_plan = self.create_node_plan(node_counter, 'replica', shard_num, master_node_id=primary_node_plan.node_id)
                nodes.append(replica_plan)
                print(f"{replica_plan.node_id}: replica, shard {shard_num}, "
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
        print(f"\nSpawning {len(node_plans)} nodes")
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
                '--cluster-replica-validity-factor', '10',
                '--cluster-migration-barrier', '1',
                '--cluster-require-full-coverage', 'no',
                '--dir', node_data_dir,
                '--logfile', log_file,
                '--loglevel', 'notice',
                '--appendonly', 'yes',
                '--appendfilename', 'appendonly.aof',
                '--save', '""',
                '--maxmemory', '500mb',
                '--maxmemory-policy', 'allkeys-lru'
            ]
            
            print(f"Spawning {plan.node_id} on port {plan.port}")
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
                print(f"Spawned {plan.node_id} with PID {process.pid}")
                
            except Exception as e:
                print(f"Failed to spawn {plan.node_id}: {e}")
                for node in node_info_list:
                    self.terminate_node(node)
                raise
        
        print(f"\nWaiting for all nodes to be ready")
        for node in node_info_list:
            deadline = time.time() + 30
            ready = False
            
            while time.time() < deadline:
                if node.process.poll() is not None:
                    print(f"Node {node.node_id} process died")
                    break
                
                try:
                    client = valkey.Valkey(
                        host='127.0.0.1',
                        port=node.port,
                        socket_timeout=1,
                        decode_responses=True
                    )
                    
                    if client.ping():
                        print(f"Node {node.node_id} is active")
                        ready = True
                        break
                except (valkey.ConnectionError, valkey.TimeoutError):
                    pass
                
                time.sleep(0.5)
            
            if not ready:
                for failed_node in node_info_list:
                    self.terminate_node(failed_node)
                raise Exception(f"Node {node.node_id} failed to start")
        
        print(f"All {len(node_info_list)} nodes are active")
        return node_info_list
    
    def terminate_node(self, node: NodeInfo) -> None:
        """Terminate a Valkey node process"""
        if node.process.poll() is not None:
            return
        
        print(f"Terminating {node.node_id} (PID {node.pid})")
        
        node.process.terminate()
        try:
            node.process.wait(timeout=5)
            print(f"{node.node_id} terminated")
        except subprocess.TimeoutExpired:
            node.process.kill()
            node.process.wait()
            print(f"{node.node_id} terminated")
    
    def cleanup_cluster(self, nodes_in_cluster: List[NodeInfo]) -> None:
        """Clean up cluster by terminating nodes and releasing resources"""
        print(f"\nCleaning up cluster {self.cluster_id}")
        
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
                print(f"Deleted data directory")
        
        print(f"Cluster {self.cluster_id} cleaned up")


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
    
    def cluster_meet(self, nodes_in_cluster: List[NodeInfo], timeout: int = 30) -> None:
        """Connect cluster nodes and wait for convergence"""
        if len(nodes_in_cluster) < 2:
            return
        
        print(f"\nConnecting {len(nodes_in_cluster)} nodes with CLUSTER MEET")
        
        first_node = nodes_in_cluster[0]
        first_node_client = self.get_client(first_node)
                
        for node in nodes_in_cluster[1:]:
            print(f"Meeting {node.node_id} (port {node.port})")
            first_node_client.execute_command('CLUSTER', 'MEET', '127.0.0.1', node.port)
            time.sleep(0.1)
        
        print("CLUSTER MEET complete")
        
        # Wait for convergence
        expected_count = len(nodes_in_cluster)
        deadline = time.time() + timeout
        
        print(f"\nWaiting for cluster convergence for all {expected_count} nodes")
        
        while time.time() < deadline:
            all_converged = True
            
            for node in nodes_in_cluster:
                try:
                    info_dict = self.get_cluster_info(node)
                    known_nodes = int(info_dict.get('cluster_known_nodes', 0))
                    
                    if known_nodes < expected_count:
                        print(f"{node.node_id} sees {known_nodes}/{expected_count} nodes")
                        all_converged = False
                    
                except Exception as e:
                    print(f"Error checking {node.node_id}: {e}")
                    all_converged = False
            
            if all_converged:
                print(f"All nodes see each other")
                return
            
            time.sleep(1)
        
        raise Exception(f"Cluster failed to converge within {timeout}s")
    
    def reset_cluster_state(self, nodes_in_cluster: List[NodeInfo]) -> None:
        """Reset cluster state on all nodes"""        
        print("\nResetting cluster state")
        for node in nodes_in_cluster:
            client = self.get_client(node)
            client.execute_command('CLUSTER', 'RESET', 'HARD')
            print(f"Reset {node.node_id}")
        
        time.sleep(1)
        print("Successfully reset the cluster")
    
    def assign_and_verify_slots(self, nodes_in_cluster: List[NodeInfo]) -> Dict[str, str]:
        """Assign hash slots to primary nodes and verify assignment"""
        primary_nodes = [node for node in nodes_in_cluster if node.role == 'primary']
        
        if not primary_nodes:
            print("No primary nodes to assign slots to")
            return {}
        
        print(f"\nAssigning slots to {len(primary_nodes)} primaries")
        
        node_ids = {}
        
        for primary in primary_nodes:
            client = self.get_client(primary)
            
            slots = list(range(primary.slot_start, primary.slot_end + 1))
            print(f"Assigning slots {primary.slot_start}-{primary.slot_end} to {primary.node_id}")
            
            client.execute_command('CLUSTER', 'ADDSLOTS', *slots)
            
            # Get cluster node ID
            cluster_node_id = client.execute_command('CLUSTER', 'MYID')
            node_ids[primary.node_id] = cluster_node_id
            primary.cluster_node_id = cluster_node_id
            
            print(f"{primary.node_id} cluster ID: {cluster_node_id[:8]}")
        
        print("Slot assignment complete")
        time.sleep(2)
        
        # Verify slots were assigned correctly
        info_dict = self.get_cluster_info(nodes_in_cluster[0])
        slots_assigned = int(info_dict.get('cluster_slots_assigned', 0))
        slots_fail = int(info_dict.get('cluster_slots_fail', 0))
        
        print(f"\nSlot verification:")
        print(f"Slots assigned: {slots_assigned}/16384")
        print(f"Slots failed: {slots_fail}")
        
        if slots_assigned != 16384 or slots_fail != 0:
            raise Exception(f"Slot assignment failed: {slots_assigned}/16384 assigned, {slots_fail} failed")
        
        return node_ids
    
    def setup_and_sync_replication(self, nodes_in_cluster: List[NodeInfo], primary_ids: Dict[str, str], timeout: int = 60) -> None:
        """Configure replication and wait for replicas to sync"""
        replicas = [node for node in nodes_in_cluster if node.role == 'replica']
        
        if not replicas:
            print("\nNo replicas to configure")
            return
        
        print(f"\nConfiguring {len(replicas)} replicas")
        
        for replica in replicas:
            master_cluster_id = primary_ids.get(replica.master_node_id)
            
            if not master_cluster_id:
                print(f"Could not find master for {replica.node_id}")
                continue
            
            print(f"Configuring {replica.node_id} to replicate {replica.master_node_id}")
            
            client = self.get_client(replica)
            client.execute_command('CLUSTER', 'REPLICATE', master_cluster_id)
            replica.cluster_node_id = client.execute_command('CLUSTER', 'MYID')
            
            print(f"Replication configured")
        
        print("Replication setup complete")
        
        print(f"\nWaiting for {len(replicas)} replicas to sync")
        deadline = time.time() + timeout
        
        while time.time() < deadline:
            all_synced = True
            
            for replica in replicas:
                try:
                    client = self.get_client(replica)
                    info = client.info('replication')
                    
                    master_link = info.get('master_link_status')
                    
                    if master_link != 'up':
                        print(f"  {replica.node_id}: {master_link}")
                        all_synced = False
                
                except Exception as e:
                    print(f"Error checking {replica.node_id}: {e}")
                    all_synced = False
            
            if all_synced:
                print("All replicas are in sync with their master!")
                return
            
            time.sleep(1)
        
        raise Exception(f"Replicas failed to sync within {timeout}s")
    
    def validate_cluster(self, nodes_in_cluster: List[NodeInfo]) -> bool:
        """Validate cluster health and configuration"""
        if not nodes_in_cluster:
            return False
        
        print("\n" + "=" * 60)
        print("CLUSTER VALIDATION")
        print("=" * 60)
        
        client = self.get_client(nodes_in_cluster[0])
        info = client.execute_command('CLUSTER', 'INFO')
        
        print(f"RAW CLUSTER INFO:\n{info}\n")

        info_dict = {}
        for line in info.split('\r\n'):
            if ':' in line:
                key, value = line.split(':', 1)
                info_dict[key] = value
        
        cluster_state = info_dict.get('cluster_state')
        slots_assigned = int(info_dict.get('cluster_slots_assigned', 0))
        slots_fail = int(info_dict.get('cluster_slots_fail', 0))
        
        print(f"Cluster state: {cluster_state}")
        print(f"Slots assigned: {slots_assigned}/16384")
        print(f"Slots failed: {slots_fail}")
        
        is_healthy = (cluster_state == 'ok' and slots_assigned == 16384 and slots_fail == 0)
        
        if is_healthy:
            print("\nCluster is Healthy")
        else:
            print("\n Cluster is Not Healthy")
                
        return is_healthy
    
    def form_cluster(self, nodes_in_cluster: List[NodeInfo]) -> bool:
        """Form a complete cluster from spawned nodes"""
        print("\n" + "=" * 60)
        print("FORMING CLUSTER")
        print("=" * 60)
        
        try:
            self.reset_cluster_state(nodes_in_cluster)
            self.cluster_meet(nodes_in_cluster)
            
            primary_ids = self.assign_and_verify_slots(nodes_in_cluster)
                       
            self.setup_and_sync_replication(nodes_in_cluster, primary_ids)
            
            return self.validate_cluster(nodes_in_cluster)
        
        except Exception as e:
            print(f"\nCluster formation failed: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def close_connections(self) -> None:
        """Close all Valkey client connections"""
        for client in self.connections.values():
            try:
                client.close()
            except:
                pass
        self.connections.clear()