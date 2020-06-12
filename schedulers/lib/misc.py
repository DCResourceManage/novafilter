class ProcessTimeDelta:
    def diff_in_microsecs(self, start_time, end_time):
        return (end_time - start_time) * 10**6


class DebugStats:
    def __init__(self):
        self.stat = {'cpu_passed': -1,
                'ram_passed': -1,
                'latency': -1,
                'vm_uuid': ''}

    def reset(self):
        self.stat = {'cpu_passed': -1,
                'ram_passed': -1,
                'latency': -1,
                'vm_uuid': ''}
