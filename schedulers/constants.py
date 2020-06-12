from typing import Dict, List, Union, Tuple, Any

SCHED_SUCCESS = 0
SCHED_FAIL = 1
SCHED_INSUFFICIENT_RSRC = 2
SCHED_RETRY_REACHED = 3
SCHED_VIOL = 4

TYPE_STR = 'type'
VDC_UUID_STR = 'vdc_uuid'
VM_UUID_STR = 'vm_uuid'
CORES_STR = 'cores'
RAM_STR = 'ram_in_gb'
SERVER_STR = 'server'
BW_STR = 'net_conns_in_mbps'

VM_CREATE_STR = 'create'
VM_DELETE_STR = 'delete'
VM_UPDATE_STR = 'update'

FAILED_STR = 'failed'

FAIL_VM_STR = 'fail_vms'
FAIL_CORES_STR = 'fail_cores'
FAIL_RAM_STR = 'fail_ram_in_gb'
FAIL_BAND_STR = 'fail_band_in_mbps'

VIOL_VM_STR = 'viol_vms'
VIOL_BAND_STR = 'viol_band_in_mbps'

STATS_STR = 'failure_stats'

RACK_STR = 'rack'
POD_STR = 'pod'
CLUSTER_STR = 'cluster'

# example: {'type': 'create', 'vdc_uuid': dep_id, 'vm_uuid': vm_id,
# 'cores': 4, 'ram_in_gb': 8}
EVENT_TYPE = Dict[str, Union[str, float]]

EVENT_LIST_TYPE = Dict[int, List[EVENT_TYPE]]
