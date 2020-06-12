import time
import json
import random
import sys
import glog

from collections import OrderedDict
from typing import Dict, List, Tuple

import schedulers.constants as const
from schedulers.lib.domain import Cluster, Server
from schedulers.lib.workload import VMEvent
from schedulers.lib.misc import ProcessTimeDelta, DebugStats

# example: {vm_uuid: (server_obj, req)}
WORKING_SET_VM_TYPE = Dict[str, Tuple[Server, VMEvent]]

class NovaFilter(object):
    """ NovaFilter implements OpenStack Nova's Filter-Weigher based scheduling.
    It allocates CPU and RAM. """

    def __init__(self, cluster: Cluster, debug: bool):
        """ Initialize NovaFilter scheduler.
        :cluster: datacenter topology to place the VMs onto
        :debug: flag to enable debug stat collection
        """
        self.cluster = cluster
        self.servers = list(self.cluster.servers.values())
        self.timedelta_obj = ProcessTimeDelta()

        self.debug = debug
        glog.info('running with debug = {}'.format(self.debug))
        if self.debug:
            self.debug_stats = OrderedDict()
            self.debug_stats_obj = DebugStats()

        # type: Dict[str, float]
        self.failure_stats = {const.FAIL_VM_STR: 0, const.FAIL_CORES_STR: 0,
                const.FAIL_RAM_STR: 0}

        # Dictionary of current VM allocations
        self.working_set_vms = dict()  # type: WORKING_SET_VM_TYPE

        # properties of the VM currently being allocated
        self.curr_req_vm = None  # type: VMEvent

        # self.allocations holds all allocated vm events as a list of dictionaries
        # Example:
        # {
        #     vm: lib.workload.VMEvent,
        #     server_id: string,
        #     timedelta: datetime.timedelta
        # }
        self.allocations = []
        self.start_time = None

    def collect_debug_stats(self):
        tick_val = self.curr_req_vm.tick
        if tick_val in self.debug_stats:
            self.debug_stats[tick_val].append({
                'cpu_passed': self.debug_stats_obj.stat['cpu_passed'],
                'ram_passed': self.debug_stats_obj.stat['ram_passed'],
                'vm_uuid': self.debug_stats_obj.stat['vm_uuid']})
        else:
            self.debug_stats[tick_val] = [{
                'cpu_passed': self.debug_stats_obj.stat['cpu_passed'],
                'ram_passed': self.debug_stats_obj.stat['ram_passed'],
                'vm_uuid': self.debug_stats_obj.stat['vm_uuid']}]

    def place_vm(self, req: VMEvent, current_server: Server):
        """ Place the VM on the give server. Also do CPU and RAM bookkeeping.
        :req: properties of to-be-allocated VM
        :current_server: a potential server for to-be-allocated VM
        :returns: status of the allocation. It is always const.SCHED_SUCCESS
        since current_server was already checked to have sufficient resources
        to accommodate the VM. """
        self.working_set_vms[req.vm_uuid] = (current_server, req)
        if current_server.allocate_cores(req.cores) != const.SCHED_SUCCESS:
            glog.error('ERROR: failed to deduct {} cores from server {}'.format(
                req.cores, current_server))
            sys.exit(1)
        if current_server.allocate_ram(req.ram) != const.SCHED_SUCCESS:
            glog.error('ERROR: failed to deduct {} RAM from server {}'.format(
                req.ram, current_server))
            sys.exit(1)

        # add the scheduled VM to the output
        end_time = time.process_time()
        timedelta = self.timedelta_obj.diff_in_microsecs(self.start_time, end_time)
        alloc = dict(server_id=current_server.id, vm=req, timedelta=timedelta)
        self.allocations.append(alloc)

        if self.debug:
            self.debug_stats_obj.stat['vm_uuid'] = req.vm_uuid

        #glog.debug('successfully allocated VM: {}'.format(req))
        return const.SCHED_SUCCESS

    def cpu_filter(self, req: VMEvent, servers: List[Server]) -> List[Server]:
        """ Filter out servers without sufficient cores.
        :req: properties of to-be-allocated VM
        :servers: list of servers to-be-filtered
        :returns: list of servers that satisfy the requirement """
        return [server for server in servers if server.has_cores_capacity(req.cores)]

    def mem_filter(self, req: VMEvent, servers: List[Server]) -> List[Server]:
        """ Filter out servers without sufficient memory.
        :req: properties of to-be-allocated VM
        :servers: list of servers to-be-filtered
        :returns: list of servers that satisfy the requirement """
        return [server for server in servers if server.has_ram_capacity(req.ram)]

    def weigher(self, req: VMEvent, servers: List[Server]) -> int:
        """ Choose a server among many servers according to weighing policy.
        Policy is the random server selection policy.
        :req: properties of to-be-allocated VM
        :servers: list of servers to-be-filtered
        :returns: the selected server. """
        # defensive programming: make sure none of the servers are const.FAILED_STR
        assert(const.FAILED_STR not in servers)
        return random.choice(servers)

    def allocate_vm(self, req: VMEvent) -> None:
        """ Place the VM on a server.
        :req: properties of to-be-allocated VM """
        #glog.debug('started allocating VM: {}'.format(req))
        if self.debug:
            self.debug_stats_obj.reset()

        self.curr_req_vm = req

        self.start_time = time.process_time()
        passed_servers = self.cpu_filter(req, self.servers)
        if self.debug:
            self.debug_stats_obj.stat['cpu_passed'] = len(passed_servers)

        if len(passed_servers) > 0:
            passed_servers = self.mem_filter(req, passed_servers)
            if self.debug:
                self.debug_stats_obj.stat['ram_passed'] = len(passed_servers)

        if len(passed_servers) > 0:
            selected_server = self.weigher(req, passed_servers)
            status = self.place_vm(req, selected_server)
            if status == const.SCHED_SUCCESS:
                if self.debug:
                    self.collect_debug_stats()
                return

        end_time = time.process_time()
        # add the failed VM to the output
        self.working_set_vms[req.vm_uuid] = (const.FAILED_STR, req)
        timedelta = self.timedelta_obj.diff_in_microsecs(self.start_time, end_time)
        alloc = dict(server_id=const.FAILED_STR, vm=req, timedelta=timedelta)
        self.allocations.append(alloc)
        # update failure_stats
        self.failure_stats[const.FAIL_VM_STR] += 1
        self.failure_stats[const.FAIL_CORES_STR] += req.cores
        self.failure_stats[const.FAIL_RAM_STR] += req.ram

    def deallocate_vm(self, req: VMEvent) -> None:
        """ Deallocate already allocated VM.
        :req: VM deallocation request """
        start_time = time.process_time()
        placed_server, allocated_vm = self.working_set_vms.get(req.vm_uuid, (None, None))
        if placed_server and allocated_vm:
            if placed_server == const.FAILED_STR:
                end_time = time.process_time()
                timedelta = self.timedelta_obj.diff_in_microsecs(start_time, end_time)
                alloc = dict(server_id=const.FAILED_STR, vm=req, timedelta=timedelta)
            else:
                # free up CPU and RAM
                if placed_server.free_cores(allocated_vm.cores) != const.SCHED_SUCCESS:
                    glog.error('ERROR: failed to add {} cores back to server {}'.format(
                        allocated_vm.cores, placed_server))
                    sys.exit(1)

                if placed_server.free_ram(allocated_vm.ram) != const.SCHED_SUCCESS:
                    glog.error('ERROR: failed to add {} ram back to server {}'.format(
                        allocated_vm.ram, placed_server))
                    sys.exit(1)

                # add the scheduled VM to the output
                end_time = time.process_time()
                timedelta = self.timedelta_obj.diff_in_microsecs(start_time, end_time)
                alloc = dict(server_id=placed_server.id, vm=req, timedelta=timedelta)
            self.allocations.append(alloc)
            self.working_set_vms.pop(req.vm_uuid)
        else:
            glog.error('to-be-deallocated VM is not found: {}. Exit.'.format(allocated_vm))
            sys.exit(1)

        return None

    def schedule(self, events: List[VMEvent]) -> int:
        """ Handle a list of (de)allocation requests
        :events: list of VM allocate and deallocate events
        :returns: status code of the scheduler. const.SCHED_SUCCESS if
        full workload is complete. const.SCHED_FAIL otherwise. """
        # process each allocate/deallocate VM event
        for req in events:
            # process VM create and delete events within this VDC
            if req.type == const.VM_CREATE_STR:
                self.allocate_vm(req)
            elif req.type == const.VM_DELETE_STR:
                self.deallocate_vm(req)
            else:
                glog.error('ERROR: unknown request type {}. Exit.'.format(req))
                sys.exit(1)

        return const.SCHED_SUCCESS

    def output_allocations(self, output_path: str) -> None:
        """ Flush the current allocations to a JSON file
        :output_path: name of the JSON file name to output
        The format looks like:
        {
            "tick_0": [ {'type': 'create', 'vdc_uuid': 'vdc1', 'vm_uuid': 'vm1', 'cores': 2.0, 'ram_in_gb': 0.75, 'server': 'p0_t0_s0'}, ...],
            "tick_1": [ {'type': 'delete', 'vdc_uuid': 'vdc1', 'vm_uuid': 'vm1', 'server': 'p0_t0_s0'}, ...],
            "tick_2": [ {...}, ...],
            ...
        }
        """
        glog.info('total resource stats for failed VMs: {}'.format(self.failure_stats))

        data = OrderedDict()  # grouping allocated vms by tick
        for alloc in self.allocations:
            # extract info from dict
            vm = alloc['vm']
            server_id = alloc['server_id']
            if vm.type == const.VM_CREATE_STR:
                # convert to output formatted dict
                alloc_vm = dict(type=vm.type, vdc_uuid=vm.vdc_uuid,
                    vm_uuid=vm.vm_uuid, cores=vm.cores, ram_in_gb=vm.ram,
                    server=server_id, elapsed_microsec=alloc['timedelta'])

            elif vm.type == const.VM_DELETE_STR:
                # delete events do not have VM cores, ram, and some other info
                alloc_vm = dict(type=vm.type, vdc_uuid=vm.vdc_uuid, vm_uuid=vm.vm_uuid,
                    server=server_id, elapsed_microsec=alloc['timedelta'])
            else:
                glog.error('ERROR: invalid event type: {}. Only supported are: create, delete. Exit.'.format(vm.type))
                sys.exit(1)
            if vm.tick not in data:
                data[vm.tick] = []
            data[vm.tick].append(alloc_vm)

        # add failure stats to the output
        data[const.STATS_STR] = self.failure_stats

        with open(output_path, 'w') as outfile:
            json.dump(data, outfile, indent=4)

        # output debug_stats
        if self.debug:
            debug_fname = '{}_debug_stats.json'.format(output_path.strip()[:-5])
            with open(debug_fname, 'w') as ds_file:
                json.dump(self.debug_stats, ds_file, indent=4)
            glog.info('wrote debugging output to {}'.format(debug_fname))

