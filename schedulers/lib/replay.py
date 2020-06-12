import json
from schedulers.lib.workload import VMEvent

class WorkloadReplayer(object):
    WORKLOAD_TICK_PREFIX = "tick_"

    def __init__(self):
        self.keys = [] # order in which we pull workload per tick from self.workload
        self.currTick = 0  # current index in keys
        self.workload = dict()

    def load_workload(self, workload_path):
        """
        Load workload from file into replayer as lib.workload.VM

        Parameters:
        workload_path (string): path to workload file
        """
        with open(workload_path) as f:
            workload = json.load(f)
            keys = workload.keys()
            # extract objects that have keys with a "tick_" prefix
            keys = list(filter(lambda k: k.startswith(WorkloadReplayer.WORKLOAD_TICK_PREFIX), keys))
            # sort keys in monotonically increasing order. the keys have timestamp suffix. e.g. tick_123456
            keys = sorted(keys, key=lambda k: int(k[len(WorkloadReplayer.WORKLOAD_TICK_PREFIX):]))
            self.keys = keys
            for tick_id, events in workload.items():
                self.workload[tick_id] = []
                for event in events:
                    self.workload[tick_id].append(VMEvent(tick_id, event["type"], event["vdc_uuid"],
                        event["vm_uuid"], event.get("cores", 0), event.get("ram_in_gb", 0.0)))

    def replay(self):
        """ Replay workload by ticks. 
        :returns: list of workloads within current tick """
        if self.currTick < len(self.keys):
            events_in_this_tick = self.keys[self.currTick]
            self.currTick += 1
            return self.workload.get(events_in_this_tick, [])
        else:
            return []

