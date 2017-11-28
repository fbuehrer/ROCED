
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
import pyslurm

from Core import Config
from RequirementAdapter.Requirement import RequirementAdapterBase
from Util import Logging, ScaleTools
from Util.PythonTools import Caching

class SlurmRequirementAdapter(RequirementAdapterBase):
    configMachines = "machines"
    configSlurmPartition = "slurm_partition"

    def __init__(self):
        """Requirement adapter, connecting to an Slurm batch system."""
        super(SlurmRequirementAdapter, self).__init__()

        self.setConfig(self.configMachines, dict())
        self.addCompulsoryConfigKeys(self.configMachines, Config.ConfigTypeDictionary)
        self.addCompulsoryConfigKeys(key=self.configSlurmPartition, datatype=Config.ConfigTypeString, description="Slurm Partition name")

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
        self.logger.info("Checking requirements in partition {}".format( self.getConfig(self.configSlurmPartition) ))

        # cmd = 'squeue -p {} --noheader --format="%T %r %c"'.format(self.getConfig( self.configSlurmPartition))
        try:
            jobs = pyslurm.job().get()
        except ValueError as e:
            self.logger.warning("Could not get Slurm queue status! %s" % e)
            return None

        jobs = {k: v for k, v in jobs.items() if v['partition'] == self.getConfig(self.configSlurmPartition)}
        if len(jobs) == 0:
            return None

        required_cpus_total = 0
        required_cpus_idle_jobs = 0
        required_cpus_running_jobs = 0
        cpus_dependency_jobs = 0

        for job in jobs.values():
            if "Dependency" in job['state_reason']:
                cpus_dependency_jobs += job['pn_min_cpus']
            elif "PartitionTimeLimit" in job['state_reason']:
                pass
            elif "PENDING" in  job['job_state']:
                required_cpus_total += job['pn_min_cpus']
                required_cpus_idle_jobs += job['pn_min_cpus']
            elif "RUNNING" in job['job_state']:
                required_cpus_total += job['pn_min_cpus']
                required_cpus_running_jobs += job['pn_min_cpus']
            elif "CANCELLED" in job['job_state']:
                pass
            else:
                self.logger.warning("Unknown job state: %s. Ignoring.", job['job_state'])


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

