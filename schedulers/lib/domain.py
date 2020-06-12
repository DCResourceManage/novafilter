import sys
import glog
import json
from collections import OrderedDict
from typing import Any
import uuid as uuid_lib

import schedulers.constants as const


class Cluster:
    def __init__(self):
        self.servers = OrderedDict()  # {server_name: server_object, ...}

    def load(self, arg: Any) -> None:
        self.load_by_json(arg)

    def load_by_json(self, json_fname: str) -> None:
        """ Loads datacenter topology into Cluster data structure.
        :json_fname: name of the file containing datacenter description """
        data = None
        with open(json_fname) as ff:
            data = json.load(ff)
        assert(data)

        # parse servers
        f_servers = data["Servers"]
        for server_id, props in f_servers.items():
            self.servers[server_id] = Server(
                id=server_id, cores=props[0], ram=props[1])


class Server(object):
    def __init__(self, id, cores, ram):
        self.id = id
        self.cores = cores
        self.ram = ram

        self._cores_remaining = cores
        self._ram_remaining = ram

    def has_cores_capacity(self, cores=0) -> bool:
        return self._cores_remaining - cores >= 0

    def has_ram_capacity(self, ram=0.0) -> bool:
        return self._ram_remaining - ram >= 0.0

    def allocate_cores(self, cores) -> int:
        """ Deduct cores from the server after VM allocation.
        :cores: number of cores being allocated
        :return: SCHED_SUCCESS if operation succeeds, SCHED_INSUFFICIENT_RSRC otherwise. """
        if cores == 0:
            return const.SCHED_INSUFFICIENT_RSRC
        if not self.has_cores_capacity(cores):
            return const.SCHED_INSUFFICIENT_RSRC
        self._cores_remaining -= cores
        return const.SCHED_SUCCESS

    def allocate_ram(self, ram) -> int:
        """ Deduct RAM from the server after VM allocation.
        :ram: amount of RAM being allocated (in MB)
        :return: SCHED_SUCCESS if operation succeeds, SCHED_INSUFFICIENT_RSRC otherwise. """
        if ram == 0:
            return const.SCHED_INSUFFICIENT_RSRC
        if not self.has_ram_capacity(ram):
            return const.SCHED_INSUFFICIENT_RSRC
        self._ram_remaining -= ram
        return const.SCHED_SUCCESS

    def free_cores(self, cores) -> int:
        """ Add cores back to the server after VM deallocation.
        :cores: number of cores being freed
        :return: SCHED_SUCCESS if operation succeeds, SCHED_FAIL otherwise. """
        self._cores_remaining += cores
        if self._cores_remaining > self.cores:
            glog.error('ERROR: invalid number of cores: {} {}'.format(
                'Server ({}) has {} cores after freeing {} cores.'.format(self.id,
                self._cores_remaining, cores),
                'Should have up to {} only.'.format(self.cores)))
            return const.SCHED_FAIL
        return const.SCHED_SUCCESS

    def free_ram(self, ram) -> int:
        """ Add RAM back to the server after VM deallocation.
        :ram: amount of RAM being freed (in MB)
        :return: SCHED_SUCCESS if operation succeeds, SCHED_FAIL otherwise. """
        self._ram_remaining += ram
        if self._ram_remaining > self.ram:
            glog.error('ERROR: invalid amount of RAM: {} {}'.format(
                'Server ({}) has {}MB RAM after freeing {}MB.'.format(self.id,
                self._ram_remaining, ram),
                'Should have up to {}MB only.'.format(self.ram)))
            return const.SCHED_FAIL
        return const.SCHED_SUCCESS

    def reset_cores(self, cores):
        self.cores = cores
        self._cores_remaining = cores

    def reset_ram(self, ram):
        self.ram = ram
        self._ram_remaining = ram

    def __repr__(self):
        return '{0.__class__.__name__}(id={0.id}, core_remain={0._cores_remaining}, ram_remain={0._ram_remaining})'.format(
            self)
