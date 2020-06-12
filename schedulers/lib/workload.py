class VMEvent(object):
    def __init__(self, tick, type, vdc_uuid, vm_uuid, cores=0, ram=0.0):
        self.tick = tick
        self.type = type
        self.vdc_uuid = vdc_uuid
        self.vm_uuid = vm_uuid
        self.cores = cores
        self.ram = ram

    def __str__(self):
        return "VMEvent: type={}, vdc_uuid={}, vm_uuid={}, cores={}, ram={}".format(
                self.type, self.vdc_uuid, self.vm_uuid, self.cores, self.ram)
