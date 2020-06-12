import argparse
import glog
import sys
import threading
import time

import schedulers.constants as const
from schedulers.lib.replay import WorkloadReplayer
from schedulers.lib.domain import Cluster

from schedulers.novafilter import NovaFilter


def _print_tick_index(replayer: WorkloadReplayer, output_freq_in_secs: int) -> None:
    """ Print progress of this scheduler on separate thread so that we know what
    step of the VDC workload being allocated.
    :replayer: an object to replay the VDC workload
    :output_freq_in_secs: frequency of the print message in seconds
    """
    time.sleep(5) # sleep for couple second for proper logging order
    while True:
        glog.info('--- processing tick # %d out of total %d ticks ---',
                replayer.currTick, len(replayer.keys))
        time.sleep(output_freq_in_secs)


def runner(scheduler, replayer):
    """ Run the scheduler until completion. """
    replayed_workload = replayer.replay()
    t1 = threading.Thread(target=_print_tick_index, args=(replayer, 1))
    t1.daemon = True
    t1.start()
    glog.info('running {}'.format(scheduler.__class__.__name__))
    while len(replayed_workload) > 0:
        scheduler.schedule(replayed_workload)
        replayed_workload = replayer.replay()
    glog.info("Full workload completed")


if __name__ == "__main__":
    """ Main driver file to run different schedulers. """
    CLI = argparse.ArgumentParser(
        description='Choose the scheduler to run with necessary options')
    group = CLI.add_mutually_exclusive_group(required=True)

    group.add_argument(
        '-nf',
        '--novafilter',
        action='store_true',
        default=False,
        help='run NovaFilter with CPU and RAM filters')

    CLI.add_argument(
        '-p',
        '--physical-network',
        default='./input/2pod_4rack_8servers.pn.json',
        help='Path to physical network JSON file')

    CLI.add_argument(
        '-w',
        '--workload',
        default='./input/workload_sample.json',
        help='Path to workload JSON file')

    CLI.add_argument(
        '-o',
        '--output',
        default="allocs.json",
        help='Path to allocation output JSON file')

    CLI.add_argument(
        '-d',
        '--debug',
        action='store_true',
        default=False,
        help='enable debug stats output')

    ARGS = CLI.parse_args()
    scheduler = None

    cluster = Cluster()
    cluster.load(ARGS.physical_network)
    rp = WorkloadReplayer()
    rp.load_workload(ARGS.workload)

    if ARGS.novafilter:
        scheduler = NovaFilter(cluster, ARGS.debug)
    else:
        glog.error('ERROR: invalid option.')
        sys.exit(1)

    runner(scheduler, rp)
    glog.info("Writing output to the file ...")
    scheduler.output_allocations(ARGS.output)
    glog.info('See {} file for allocation results'.format(ARGS.output))
