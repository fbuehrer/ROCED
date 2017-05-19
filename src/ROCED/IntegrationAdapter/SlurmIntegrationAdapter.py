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
import re
from collections import defaultdict
from datetime import datetime

from Core import MachineRegistry, Config
from IntegrationAdapter.Integration import IntegrationAdapterBase
from Util import ScaleTools
from Util.PythonTools import Caching


class SlurmIntegrationAdapter(IntegrationAdapterBase):
    configIntLogger = "logger_name"
    configSlurmName = "site_name"
    configSlurmConstraint = "slurm_constraint"
    configSlurmUser = "slurm_user"
    configSlurmKey = "slurm_key"
    configSlurmServer = "slurm_server"
    configSlurmWaitPD = "slurm_wait_pd"
    configSlurmWaitWorking = "slurm_wait_working"
    configSlurmDeadline = "slurm_deadline"

    # list of the different slot states for each machine, e.g. [slot1,slot2,...]
    reg_site_slurm_status = "slurm_slot_status"
    reg_status_last_update = MachineRegistry.MachineRegistry.regStatusLastUpdate
    # possible slot state
    slurmStatusClaimed = "Claimed"
    # Both states show an empty/idling machine. "Owner" means that there are some job requirements
    # defined on the machine which have to be met, before a job is assigned.
    # "Unclaimed" machines will accept any job.
    slurmStatusOwner = "Owner"
    slurmStatusUnclaimed = "Unclaimed"
    slurmStatusIdle = [slurmStatusOwner, slurmStatusUnclaimed]
    slurmStatusRetiring = "Retiring"
    # possible slot activity
    slurmActivityDrained = "Drained"
    # slurm machine name saved in machine registry - communication to site adapter(s)
    reg_site_server_node_name = "reg_site_server_node_name"

    # Output and its parsing
    _query_format_string = "-autoformat: Machine State Activity"
    regex_queue_parser = re.compile("([a-z-0-9]+).* ([a-zA-Z]+) ([a-zA-Z]+)", re.MULTILINE)
    collector_error_string = "Failed to end classad message"

    def __init__(self):
        """Slurm specific integration adapter. Monitors collector via slurm_status and updates machine states.

        Load config keys from config file

        :return:
        """
        super(SlurmIntegrationAdapter, self).__init__()
        self.addOptionalConfigKeys(self.configIntLogger, Config.ConfigTypeString,
                                   description="logger name",
                                   default="Slurm_Int")
        self.addOptionalConfigKeys(self.configSlurmConstraint, Config.ConfigTypeString,
                                   description="ClassAd constraint to filter machines in slurm_status",
                                   default="True")
        self.addOptionalConfigKeys(key=self.configSlurmUser, datatype=Config.ConfigTypeString,
                                   description="Login name for slurm collector server.",
                                   default=getpass.getuser())
        self.addOptionalConfigKeys(key=self.configSlurmServer, datatype=Config.ConfigTypeString,
                                   description="Hostname of collector server. If machines are connected to connector "
                                               "and have commandline interface installed, localhost can easily be used "
                                               "because we query with \"global\".",
                                   default="localhost")
        self.addOptionalConfigKeys(key=self.configSlurmKey, datatype=Config.ConfigTypeString,
                                   description="Path to SSH key for remote login. Not necessary with server localhost.",
                                   default="~/")
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
            if machine[cls.reg_site_slurm_status][slot][0] in cls.slurmStatusClaimed:
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
                if slot[0] == cls.slurmActivityDrained:
                    nDrainedSlots += 1
                    statusDraining = True
                if slot[1] == cls.slurmStatusRetiring:
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

        # check machine registry
        for mid in self.mr.getMachines(self.siteName):
            machine_ = self.mr.machines[mid]

            # Is an "Integrating" machine completely started up? (appears in slurm) -> "Working"
            if machine_[self.mr.regStatus] == self.mr.statusIntegrating:
                if machine_[self.reg_site_server_node_name] in slurm_machines:
                    self.mr.updateMachineStatus(mid, self.mr.statusWorking)
                    # number of cores = number of slots
                    self.mr.machines[mid][self.reg_site_slurm_status] = slurm_machines[
                        machine_[self.reg_site_server_node_name]]
                    self.mr.machines[mid][self.mr.regMachineCores] = len(
                        self.mr.machines[mid][self.reg_site_slurm_status])
                # Machine stuck integrating? -> Disintegrated
                elif self.mr.calcLastStateChange(mid) > slurm_timeout:
                    self.mr.updateMachineStatus(mid, self.mr.statusDisintegrated)

            # "Working" machines need machine load > 0.01, otherwise they are "unclaimed".
            # -> "pending disintegration"
            if machine_[self.mr.regStatus] == self.mr.statusWorking:
                if machine_[self.reg_site_server_node_name] in slurm_machines:
                    # update slurm slot status & calculate machine load
                    self.mr.machines[mid][self.reg_site_slurm_status] = slurm_machines[
                        machine_[self.reg_site_server_node_name]]
                    if self.calcMachineLoad(mid) <= 0.01 and self.mr.calcLastStateChange(mid) > slurm_wait_working:
                        self.mr.updateMachineStatus(mid, self.mr.statusPendingDisintegration)
                    # If slot activity/machine state indicate draining -> Pending Disintegration
                    if self.calcDrainStatus(mid)[1] is True:
                        self.mr.updateMachineStatus(mid, self.mr.statusPendingDisintegration)
                else:
                    # Machine disappeared
                    self.mr.updateMachineStatus(mid, self.mr.statusDisintegrating)

            # check if machines pending disintegration can be (disintegrating) or were shut down
            # (disintegrated)
            elif machine_[self.mr.regStatus] == self.mr.statusPendingDisintegration:
                # is machine (still) listed in slurm machines (search for "slurm name")?
                if self.reg_site_server_node_name in machine_:
                    if machine_[self.reg_site_server_node_name] in slurm_machines:
                        # update slurm slot status & calculate machine load
                        self.mr.machines[mid][self.reg_site_slurm_status] = slurm_machines[
                            machine_[self.reg_site_server_node_name]]
                        self.calcMachineLoad(mid)

                        # machine load > 0.01 -> at least one slot is claimed -> re-enable
                        # TODO: Switch to an integer "cores_claimed" and compare > 0
                        if self.mr.machines[mid][self.mr.regMachineLoad] > 0.01:
                            # Only re-enable non-draining nodes
                            if self.calcDrainStatus(mid)[1] is False:
                                self.mr.updateMachineStatus(mid, self.mr.statusWorking)
                        elif self.mr.calcLastStateChange(mid) > slurm_wait_PD:
                            self.mr.updateMachineStatus(mid, self.mr.statusDisintegrating)
                    else:
                        self.mr.updateMachineStatus(mid, self.mr.statusDisintegrating)
                else:
                    self.mr.updateMachineStatus(mid, self.mr.statusDisintegrating)

            # "Disintegrating": -> Shutdown should be started (by site adapter)
            # # If it's not listed in slurm, it's done shutting down -> "disintegrated"
            if machine_[self.mr.regStatus] == self.mr.statusDisintegrating:
                if (machine_[self.reg_site_server_node_name] not in slurm_machines or
                            self.mr.calcLastStateChange(mid) > slurm_timeout):
                    self.mr.updateMachineStatus(mid, self.mr.statusDisintegrated)

        self.logger.debug("Content of machine registry:\n%s" % self.getSiteMachines())
        self.logger.debug("Content of slurm machines:\n%s" % slurm_machines.items())

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

    def parse_sinfo_output(self,output):
        nlist = []
        for line in output.splitlines():
            if line.split(',')[0] not in nlist:
                nlist.append(line.split(','))
        return nlist





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

        # load the connection settings from config
        slurm_server = self.getConfig(self.configSlurmServer)
        slurm_user = self.getConfig(self.configSlurmUser)
        slurm_key = self.getConfig(self.configSlurmKey)
        slurm_constraint = self.getConfig(self.configSlurmConstraint)
        slurm_ssh = ScaleTools.Ssh(slurm_server, slurm_user, slurm_key)

        cmd = ("sinfo -h -l -N --format %n,%C -p nemo_vm_altjak")

        # get a list of the slurm machines (SSH)
        slurm_result = slurm_ssh.handleSshCall(call=cmd, quiet=True)
        slurm_ssh.debugOutput(self.logger, "EKP-manage", slurm_result)

        if slurm_result[0] != 0:
            raise ValueError("SSH connection to Slurm collector could not be established.")
        elif self.collector_error_string in slurm_result[1]:
            raise ValueError("Collector(s) didn't answer.")

        # prepare list of slurm machines
        logging.debug("Trying to parse sinfo output" )
        tmp_slurm_machines=self.parse_sinfo_output(slurm_result[1])

        slurm_machines = defaultdict(list)
        logging.debug(tmp_slurm_machines)
        for node in tmp_slurm_machines:
            logging.debug(node)
            machine_name=node[0]
            cpus=node[1].split('/')
            i=0
            logging.debug(node)
            for slot in range(int(cpus[-1])):
                state='idle'
                activity=None
                if i<int(cpus[0]): state='allocated'
                slurm_machines[machine_name].append([state, activity])
                i+=1

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
