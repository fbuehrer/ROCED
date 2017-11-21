# ===============================================================================
#
# Copyright (c) 2015, 2016 by Guenther Erli, Frank Fischer and Georg Fleig
#
# This file is part of ROCED.
#
# ROCED is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# ROCED is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with ROCED.  If not, see <http://www.gnu.org/licenses/>.
#
# ===============================================================================
from __future__ import unicode_literals, absolute_import

import getpass
import logging
import pprint
import pyslurm
from collections import defaultdict
from datetime import datetime

from Core import MachineRegistry, Config
from IntegrationAdapter.Integration import IntegrationAdapterBase
from Util import ScaleTools
from Util.PythonTools import Caching


class SlurmIntegrationAdapter(IntegrationAdapterBase):
    configIntLogger = "logger_name"
    configSlurmName = "site_name"
    configSlurmWaitPD = "slurm_wait_pd"
    configSlurmPartition = "slurm_partition"
    configSlurmWaitWorking = "slurm_wait_working"
    configSlurmDeadline = "slurm_deadline"

    # list of the different slot states for each machine, e.g. [slot1,slot2,...]
    reg_site_slurm_status = "slurm_slot_status"
    reg_status_last_update = MachineRegistry.MachineRegistry.regStatusLastUpdate
    # possible slot state
    slurmStatusAllocated = "allocated"
    # Both states show an empty/idling machine. "Owner" means that there are some job requirements
    # defined on the machine which have to be met, before a job is assigned.
    # "Unclaimed" machines will accept any job.
    #slurmStatusOwner = "Owner"
    #slurmStatusUnclaimed = "Unclaimed"
    slurmStatusIdle = "idle" #[slurmStatusOwner, slurmStatusUnclaimed]
    #slurmStatusRetiring = "Retiring"
    # possible slot activity
    slurmStatusDraining = "draining"
    slurmStatusDrained = "drained"
    # slurm machine name saved in machine registry - communication to site adapter(s)
    reg_site_server_node_name = "reg_site_server_node_name"

    # Output and its parsing
    #_query_format_string = "-autoformat: Machine State Activity"
    #regex_queue_parser = re.compile("([a-z-0-9]+).* ([a-zA-Z]+) ([a-zA-Z]+)", re.MULTILINE)
    #collector_error_string = "Failed to end classad message"

    def __init__(self):
        """Slurm specific integration adapter. Monitors collector via slurm_status and updates machine states.

        Load config keys from config file

        :return:
        """
        super(SlurmIntegrationAdapter, self).__init__()
        self.addOptionalConfigKeys(self.configIntLogger, Config.ConfigTypeString,
                                   description="logger name",
                                   default="Slurm_Int")
        self.addCompulsoryConfigKeys(self.configSlurmPartition, Config.ConfigTypeString, description="Slurm Partition name")
        self.addCompulsoryConfigKeys(self.configSlurmName, Config.ConfigTypeString, description="Site name")
        self.addOptionalConfigKeys(self.configSlurmWaitPD, Config.ConfigTypeInt,
                                   description="Wait for x minutes before changing to disintegrating.",
                                   default=0)
        self.addOptionalConfigKeys(self.configSlurmWaitWorking, Config.ConfigTypeInt,
                                   description="Wait for x minutes before changing to pending disintegration.",
                                   default=0)
        self.addCompulsoryConfigKeys(self.configSlurmDeadline, Config.ConfigTypeInt,
                                     description="Timeout (in minutes) before a machine stuck in "
                                                 "status integrating/disintegrating is considered lost.")

        self.logger = logging.getLogger(self.getConfig(self.configIntLogger))

    def init(self):
        """Register logger and listener

        :return:
        """
        super(SlurmIntegrationAdapter, self).init()
        self.mr.registerListener(self)

    @classmethod
    def calcMachineLoad(cls, machine_id):
        # type: (dict) -> float
        """Calculate machine load [interval (0,1)] & update object accordingly.

        Go over all job slots and check if they are (un)claimed.
        Function is made available externally since site adapters may require this information to
        terminate machines accordingly.

        :param machine_id: Single machine registry entry
        :type machine_id: str
        :return: float
        """
        machine = cls.mr.machines[machine_id]
        cores_claimed = 0.0
        machine[cls.mr.regMachineLoad] = 0.0
        for slot in range(len(machine[cls.reg_site_slurm_status])):
            if machine[cls.reg_site_slurm_status][slot][0] in cls.slurmStatusAllocated:
                cores_claimed += 1
                # set a timestamp on this event
                machine[cls.reg_status_last_update] = datetime.now()
                # update machine load in machine object
                machineLoad = cores_claimed / len(machine[cls.reg_site_slurm_status])
                machine[cls.mr.regMachineLoad] = machineLoad
        return machine[cls.mr.regMachineLoad]

    @classmethod
    def calcDrainStatus(cls, machine_id):
        # type: (dict) -> Tuple(int, bool)
        """Calculate machine drain status (number of draining slots and bool for drain status).

        :param machine_id:
        :return Tuple(int, bool):
        """
        nDrainedSlots = 0
        statusDraining = False
        try:
            for slot in cls.mr.machines[machine_id][cls.reg_site_slurm_status]:
                if cls.slurmStatusDraining in slot[0]  or cls.slurmStatusDrained in slot[0]:
                    nDrainedSlots += 1
                    statusDraining = True
        except KeyError:
            pass
        return nDrainedSlots, statusDraining

    @property
    def siteName(self):
        """Get site name of OpenStack site

        :return: site_name
        """
        return self.getConfig(self.configSlurmName)

    def getSiteMachines(self, status=None, machineType=None):
        """Get machines running at site

        :param status:
        :param machineType:
        :return: machine_registry
        """
        return self.mr.getMachines(self.siteName, status, machineType)

    def getSlurmHostname(self, ip):
        return 'host-'+ip.replace('.','-')

    def manage(self):
        """Manage machine status

        Called every cycle to check on machine registry and change machine status.

        Possible status changes, depending on config, timeouts, variables, workload, etc.:
        ---
        integrating            -> pending disintegration | working
        ---
        pending disintegration -> disintegrating | working
        working                -> pending disintegration
        disintegrating         -> disintegrated

        :return:
        """

        slurm_timeout = self.getConfig(self.configSlurmDeadline) * 60
        slurm_wait_working = self.getConfig(self.configSlurmWaitWorking) * 60
        slurm_wait_PD = self.getConfig(self.configSlurmWaitPD) * 60

        try:
            slurm_machines = self.slurmList
            if slurm_machines is None or len(self.mr.getMachines(self.siteName)) == 0:
                raise ValueError
        except ValueError as err:
            if str(err):
                self.logger.warning(err)
            self.logger.debug("Content of machine registry:\n%s" % self.getSiteMachines())
            return None

        self.logger.debug("Slurm Machines: %s", slurm_machines)
        #self.logger.debug("Machine regestry=")# % self.mr.getMachines(self.siteName))
        #pprint(self.mr.getMachines(self.siteName))
        # check machine registry
        for mid in self.mr.getMachines(self.siteName):
            machine_ = self.mr.machines[mid]

            # Is an "Integrating" machine completely started up? (appears in slurm) -> "Working"
            if machine_[self.mr.regStatus] == self.mr.statusIntegrating:
                self.logger.debug("Node in Moab=%s", machine_[self.reg_site_server_node_name])
                if self.getSlurmHostname(machine_[self.mr.regHostIp]) in slurm_machines:
                    self.logger.debug("Found Node in Slurm List :-)")
                    self.logger.debug("Working machine -> statusWorking")
                    self.mr.updateMachineStatus(mid, self.mr.statusWorking)
                    # number of cores = number of slots
                    self.mr.machines[mid][self.reg_site_slurm_status] = slurm_machines[self.getSlurmHostname(machine_[self.mr.regHostIp])]
                    self.mr.machines[mid][self.mr.regMachineCores] = len(self.mr.machines[mid][self.reg_site_slurm_status])
                # Machine stuck integrating? -> Disintegrated
                elif self.mr.calcLastStateChange(mid) > slurm_timeout:
                    self.logger.debug("Working stuck integrating -> statusDisintegrated")
                    self.mr.updateMachineStatus(mid, self.mr.statusDisintegrated)

            if machine_[self.mr.regStatus] == self.mr.statusWorking:
                if self.getSlurmHostname(machine_[self.mr.regHostIp]) in slurm_machines:
                    # update slurm slot status
                    self.logger.debug("update slurm slot status: %s" % slurm_machines[self.getSlurmHostname(machine_[self.mr.regHostIp])])
                    self.mr.machines[mid][self.reg_site_slurm_status] = slurm_machines[self.getSlurmHostname(machine_[self.mr.regHostIp])]

                    # Update machineLoad
                    load = self.calcMachineLoad(mid)
                    self.logger.debug("Machine load: %s" % load)

                    #if self.calcMachineLoad(mid) <= 0.01 and self.mr.calcLastStateChange(mid) > slurm_wait_working:
                    #    self.logger.debug("Working machine but without load -> statusPendingDisintegration")
                    #    self.mr.updateMachineStatus(mid, self.mr.statusPendingDisintegration)

                    # If slot activity/machine state indicate draining -> Pending Disintegration
                    if self.calcDrainStatus(mid)[1] is True:
                        self.logger.debug("Working machine but draining -> statusPendingDisintegration")
                        self.mr.updateMachineStatus(mid, self.mr.statusPendingDisintegration)
                #else: the machine might belong to a different partition and nothing should be done with it

                #else: just do not do anything??
                    # Machine disappeared : this appears to be problematic with different partitions !!
                    # because the if condition fails: since the machine might be working but does not
                    # appear in to slurm_machines (why exactly?)
                    # maybe this is just wrong indentation??
                #    self.logger.debug("Working machine disappeared -> statusDisintegrating")
                #    self.mr.updateMachineStatus(mid, self.mr.statusDisintegrating)

            # check if machines pending disintegration can be (disintegrating) or were shut down
            # (disintegrated)
            if machine_[self.mr.regStatus] == self.mr.statusPendingDisintegration:
                # is machine (still) listed in slurm machines?
                if self.getSlurmHostname(machine_[self.mr.regHostIp]) in slurm_machines:
                    self.logger.debug("Draining machine is still up")
                else:
                    self.logger.debug("PendingDisintegration -> statusDisintegrating")
                    self.mr.updateMachineStatus(mid, self.mr.statusDisintegrating)

                #if self.reg_site_server_node_name in machine_:
                #    if self.getSlurmHostname(machine_[self.mr.regHostIp]) in slurm_machines:
                #        # update slurm slot status & calculate machine load
                #        self.mr.machines[mid][self.reg_site_slurm_status] = slurm_machines[
                #            machine_[self.reg_site_server_node_name]]
                #        self.calcMachineLoad(mid)

                        # machine load > 0.01 -> at least one slot is claimed -> re-enable
                        # TODO: Switch to an integer "cores_claimed" and compare > 0
                #        if self.mr.machines[mid][self.mr.regMachineLoad] > 0.01:
                #            # Only re-enable non-draining nodes
                #            if self.calcDrainStatus(mid)[1] is False:
                #                self.logger.debug("PendingDisintegration but with load -> statusWorking")
                #                self.mr.updateMachineStatus(mid, self.mr.statusWorking)
                #        elif self.mr.calcLastStateChange(mid) > slurm_wait_PD:
                #            self.logger.debug("PendingDisintegration 1 -> statusDisintegrating")
                #            self.mr.updateMachineStatus(mid, self.mr.statusDisintegrating)
                #    else:
                #        self.logger.debug("PendingDisintegration 2 -> statusDisintegrating")
                #        self.mr.updateMachineStatus(mid, self.mr.statusDisintegrating)
                #else:
                #    self.logger.debug("PendingDisintegration 3 -> statusDisintegrating")
                #    self.mr.updateMachineStatus(mid, self.mr.statusDisintegrating)

            # "Disintegrating": -> Shutdown should be started (by site adapter)
            # # If it's not listed in slurm, it's done shutting down -> "disintegrated"
            if machine_[self.mr.regStatus] == self.mr.statusDisintegrating:
                #if (self.getSlurmHostname(machine_[self.mr.regHostIp]) not in slurm_machines or self.mr.calcLastStateChange(mid) > slurm_timeout):
                self.logger.debug("Machine is gone statusDisintegrating -> statusDisintegrated")
                self.mr.updateMachineStatus(mid, self.mr.statusDisintegrated)

        self.logger.debug("Content of machine registry:\n%s" % pprint.pformat(self.getSiteMachines()))
        self.logger.debug("Content of slurm machines:\n%s" % pprint.pformat(slurm_machines.items()))

    def onEvent(self, evt):
        """Event handler

        Handle machine status changes. Called every time a machine status changes.

        :param evt:
        :return:
        """
        if isinstance(evt, MachineRegistry.StatusChangedEvent):
            # machines in status up are set to integrating
            if evt.newStatus == self.mr.statusUp:
                if self.mr.machines[evt.id].get(self.mr.regSite) == self.siteName:
                    self.mr.updateMachineStatus(evt.id, self.mr.statusIntegrating)

    @property
    def description(self):
        return "SlurmIntegrationAdapter"

    @property
    @Caching(validityPeriod=-1, redundancyPeriod=900)
    def slurmList(self):
        # type: () -> Defaultdict(List)
        """Return list of slurm machines {machine name : [[state, activity], [state, activity], ..]}

        :return: slurm_machines
        """
        # load the partition info from config
        slurm_partition = self.getConfig(self.configSlurmPartition)


        self.logger.debug("Getting slurmList for jobs requested on partition {}".format(slurm_partition))

        #this outputs those nodes which are used by a job of this queue. It ignores most "down", "drained" and "draining" machines
        #in addition, one could just get the machines which are draining or drained. It might not matter which service cancels the job
        #first determine the list of nodes, then make the ssh call for each of them again for sinfo

        # Find all nodes assigned a job in this particular slurm_partition queue
        #cmd = ("squeue -p {} -h --format=%N  | sort | uniq".format(slurm_partition))

        slurm_machines = defaultdict(list)

        node_list = set()
        try:
            jobs = pyslurm.job().get()
        except ValueError as e:
            self.logger.warning("Could not get slurm queue status! {}".format(e))
            return slurm_machines

        jobs = {k: v for k, v in jobs.items() if v['partition'] == slurm_partition}
        for job in jobs.values():
            node_list.add(job['nodes'])
        node_list.discard(None)  # discard jobs which are not assigned to a node yet
        node_list = sorted(node_list)  # sort and convert to list

        self.logger.debug("Querying information for these nodes {}".format(nodes_this_partition))

        try:
            slurm_node_info = pyslurm.node().get()
        except ValueError as e:
            self.logger.warning("Could not get node info! {}".format(e))
            return slurm_machines

        for machine_name in node_list:
            node_info = slurm_node_info[machine_name]

            totalCPUs = node_info['cpus']
            allocatedCPUs = node_info['alloc_cpus']
            idleCPUs = totalCPUs - allocatedCPUs  # cannot retrieve directly from pyslurm
            state = node_info['state']

            # ignore down machines:
            if "DOWN" in state:
                continue

            for slot in range(totalCPUs):
                if allocatedCPUs > 0:
                    slurm_machines[machine_name].append(['allocated', None])
                    allocatedCPUs -= 1
                elif idleCPUs > 0:
                    slurm_machines[machine_name].append(['idle', None])
                    idleCPUs -= 1
                elif "DRAINING" in state:
                    slurm_machines[machine_name].append(['draining', None])
                elif "DRAINED" in state:
                    slurm_machines[machine_name].append(['drained', None])
                else:
                    print "WARNING!!! this is a bug!"

        return slurm_machines


    @classmethod
    def drainMachine(cls, mid):
        # type: (dict) -> None
        """ Send "slurm_drain" command to machine (draining machines won't accept new jobs).

        This usually happens in preparation of shutting the machine down. slurm_drain is an
        administrative command, so slurm_user requires slurm admin access rights."""
        # TODO: Implement "slurm_drain"
        # TODO: Must be class method (external call!); access instance attribute slurm_server...
        if cls.calcDrainStatus(mid)[1] is True:
            logging.debug("Machine is already in drain mode.")
        logging.warning("Send draining command to VM not yet implemented.")
