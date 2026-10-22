import logging
import random
from typing import Optional

import valkey
from valkey.cluster import ClusterNode

from ..models import ClusterConnection, TopologyType
from .crc16_slot_table import CRC16_SLOT_TABLE

logger = logging.getLogger()


def load_all_slots(
    cluster_connection: ClusterConnection,
    keys_per_slot: int = 10,
    rng: Optional[random.Random] = None,
) -> bool:
    """Load keys into all 16384 slots using CRC16 slot table"""
    if cluster_connection.topology_type != TopologyType.CLUSTER:
        logger.info(f"Skipping slot preload for {cluster_connection.topology_type.value} topology")
        return True

    random_source = rng if rng is not None else random.Random()
    startup_nodes = [ClusterNode(host=node["host"], port=node["port"]) for node in cluster_connection.startup_nodes]

    cluster_client = valkey.ValkeyCluster(
        startup_nodes=startup_nodes, decode_responses=True, skip_full_coverage_check=True
    )

    # Generate keys using CRC16 slot table
    slot_table_len = len(CRC16_SLOT_TABLE)
    total_keys = keys_per_slot * 16384

    keys = []
    for i in range(total_keys):
        table_key = CRC16_SLOT_TABLE[i % slot_table_len]
        key = f"key:{{{table_key}}}:{random_source.randint(0, 1000)}"
        keys.append(key)

    try:
        success_count = 0

        logger.info(f"Loading {total_keys} keys ({keys_per_slot} per slot) into cluster")

        for _i, key in enumerate(keys):
            try:
                cluster_client.set(key, "test_value")
                success_count += 1

            except Exception as e:
                logger.warning(f"Failed to set key {key}: {e}")

        if success_count == total_keys:
            logger.info(f"Successfully loaded {success_count}/{total_keys} keys")
        else:
            logger.info("Failed to load all slots with data")

        return success_count == total_keys

    except Exception as e:
        logger.error(f"Failed to load slots: {e}")
        return False
    finally:
        cluster_client.close()
