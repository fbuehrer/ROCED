
# ===============================================================================
#
# Copyright (c) 2015 by Konrad Meier, Georg Fleig
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

from Core import Config
from RequirementAdapter.Requirement import RequirementAdapterBase
from Util import Logging, ScaleTools
from Util.PythonTools import Caching

class SlurmRequirementAdapter(RequirementAdapterBase):
    configMachines = "machines"
    configSlurmUser = "slurm_user"
    configSlurmKey = "slurm_key"
    configSlurmServer = "slurm_server"
    configSlurmRequirement = "slurm_requirement"
    configSlurmConstraint = "slurm_constraint"
    configSlurmPartition = "slurm_partition"

    # See https://htcondor-wiki.cs.wisc.edu/index.cgi/wiki?p=MagicNumbers
    condorStatusIdle = 1
    condorStatusRunning = 2

    # class constants for condor_q query:
    _query_constraints = "RoutedToJobId =?= undefined && ( JobStatus == %d || JobStatus == %d )" % \
                         (condorStatusIdle, condorStatusRunning)
    # auto-format string: raw output, separated by comma
    _query_format_string = "-autoformat:r, JobStatus RequestCpus Requirements"

    _CLI_error_strings = frozenset(("Failed to fetch ads from", "Failed to end classad message"))

    def __init__(self):
        """Requirement adapter, connecting to an Slurm batch system."""
        super(SlurmRequirementAdapter, self).__init__()

        self.setConfig(self.configMachines, dict())
        self.addCompulsoryConfigKeys(self.configMachines, Config.ConfigTypeDictionary)
        self.addCompulsoryConfigKeys(key=self.configSlurmPartition, datatype=Config.ConfigTypeString, description="Slurm Partition name")
        self.addOptionalConfigKeys(key=self.configSlurmUser, datatype=Config.ConfigTypeString,
                                   description="Login name for slurm collector server.",
                                   default=getpass.getuser())
        self.addOptionalConfigKeys(key=self.configSlurmServer, datatype=Config.ConfigTypeString,
                                   description="Hostname of collector server. If machines are connected to connector "
                                               "and have commandline interface installed, localhost can easily be used "
                                               "because we query with \"global\".",
                                   default="localhost")
        self.addOptionalConfigKeys(key=self.configSlurmKey, datatype=Config.ConfigTypeString,
                                   description="Path to SSH key for remote login (not necessary with localhost).",
                                   default="~/")
        self.addOptionalConfigKeys(key=self.configSlurmRequirement, datatype=Config.ConfigTypeString,
                                   description="Grep filter string on ClassAd Requirement expression",
                                   default="")
        self.addOptionalConfigKeys(key=self.configSlurmConstraint, datatype=Config.ConfigTypeString,
                                   description="ClassAd constraint in condor_q expression",
                                   default="True")

        self.logger = logging.getLogger("SlurmReq")
        self.__str__ = self.description

    def init(self):
        super(SlurmRequirementAdapter, self).init()

    @property
    def description(self):
        return "SlurmRequirementAdapter"

    @property
    @Caching(validityPeriod=-1, redundancyPeriod=900)
    def requirement(self):
        ssh = ScaleTools.Ssh(host=self.getConfig(self.configSlurmServer),
                             username=self.getConfig(self.configSlurmUser),
                             key=self.getConfig(self.configSlurmKey))

        # Target.Requirements can't be filtered with -constraints since it would require ClassAd based regex matching.
        # TODO: Find a more generic way to match resources/requirements (condor_q -slotads ??)
        # cmd_idle = "condor_q -constraint 'JobStatus == 1' -slotads slotads_bwforcluster " \
        #            "-analyze:summary,reverse | tail -n1 | awk -F ' ' " \
        #            "'{print $3 "\n" $4}'| sort -n | head -n1"
        #constraint = "( %s ) && ( %s )" % (self._query_constraints, self.getConfig(self.configCondorConstraint))

        #cmd = ("condor_q -global -allusers -nobatch -constraint '%s' %s" % (constraint, self._query_format_string))
        #cmd = 'squeue -p nemo_vm_atlsch --noheader --format="%T %r %c"'
        self.logger.info("Checking requirements in partition {}".format( self.getConfig(self.configSlurmPartition) ))
        cmd = 'squeue -p {} --noheader --format="%T %r %c"'.format(self.getConfig( self.configSlurmPartition))
        result = ssh.handleSshCall(call=cmd, quiet=True)
        if result[0] != 0:
            self.logger.warning("Could not get Slurm queue status! %d: %s" % (result[0], result[2]))
            return None
        elif any(error_string in result[1] for error_string in self._CLI_error_strings):
            self.logger.warning("squeue request timed out.")
            return None


        required_cpus_total = 0
        required_cpus_idle_jobs = 0
        required_cpus_running_jobs = 0
        cpus_dependency_jobs = 0

        for line in result[1].splitlines():
            values = line.split()
            #self.logger.debug(values)

            if len(values) != 3:
                continue

            if "Dependency" in values[1]:
                cpus_dependency_jobs = cpus_dependency_jobs + int(values[2])
                continue
	    if "PartitionTimeLimit" in values[1]:
		continue
            elif "PENDING" in  values[0]:
                required_cpus_total = required_cpus_total + int(values[2])
                required_cpus_idle_jobs = required_cpus_idle_jobs + int(values[2])
                continue
            elif "RUNNING" in values[0]:
                required_cpus_total = required_cpus_total + int(values[2])
                required_cpus_running_jobs = required_cpus_running_jobs + int(values[2])
                continue
            else:
                self.logger.warning("unknown job state: %s. Ignoring.", values[0])
             


        self.logger.debug("Slurm queue: Idle: %d; Running: %d. in partition: %s." %
                          (required_cpus_idle_jobs, required_cpus_running_jobs, self.getConfig(self.configSlurmPartition) ))

        # cores->machines: machine definition required for RequirementAdapter
        n_cores = - int(self.getConfig(self.configMachines)[self.getNeededMachineType()]["cores"])
        self._curRequirement = - (required_cpus_total // n_cores)

	self.logger.debug("Required CPUs total=%s" % required_cpus_total)
        self.logger.debug("Required CPUs idle Jobs=%s" % required_cpus_idle_jobs)
        self.logger.debug("Required CPUs running Jobs=%s" % required_cpus_running_jobs)
        self.logger.debug("CPUs dependency Jobs=%s" % cpus_dependency_jobs)
        with Logging.JsonLog() as json_log:
            json_log.addItem(self.getNeededMachineType(), "jobs_idle", required_cpus_idle_jobs)
            json_log.addItem(self.getNeededMachineType(), "jobs_running", required_cpus_running_jobs)

        return self._curRequirement

    def getNeededMachineType(self):
        # TODO: Handle multiple machine types!
        machineType = list(self.getConfig(self.configMachines).keys())[0]
        if machineType:
            return machineType
        else:
            self.logger.error("No machine type defined for requirement.")

