
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
                # Get the number of jobs in a job array.
                # For running jobs this is no problem, because each job in a job array gets its own job id.
                # However, for pending job arrays all jobs are grouped with one ID.
                # The only possiblity to extract the number of jobs in a job array is to parse the 'array_task_str' property.
                job_array_count = self.get_job_array_count(job['array_task_str'])
                required_cpus_total += job['pn_min_cpus'] * job_array_count
                required_cpus_idle_jobs += job['pn_min_cpus'] * job_array_count
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

    def get_job_array_count(self, job_array_str):
        """Parse the 'array_task_str' property and extract the number of jobs in a job array.

        Job arrays can be specified by a list of individual job array indices or ranges of job array indices.
        The list is separated by commas (,) and the ranges are given with a minus sign (-).
        Furthermore, the maximum number of jobs which should run simultaneosly can be specified with a percent sign (%)
        followed by the number of simultaneous jobs.

        This list shows some example job array specifications and the number of jobs this function will return:

          - "1-20"        -> 20
          - "1-10,15-20"  -> 16
          - "1,3,5"       ->  3
          - "1-7%3"       ->  3
          - "1-7,10-15%3" ->  3
          - "1,3,5,7%3"   ->  3
          - None          ->  1   # job is not a job array


        More information about job array can be found at https://slurm.schedmd.com/job_array.html
        """
        # if job is no job array the 'array_task_str' property is None
        if job_array_str is None:
            return 1

        # check if number of concurrent jobs is set by user
        if "%" in job_array_str:
            return int(job_array_str.split("%")[1])

        # parse list of ranges
        total_count = 0
        for job_range in job_array_str.split(","):
            if "-" in job_range:
                job_min, job_max = job_range.split("-")
                total_count += int(job_max) - int(job_min) + 1
            else:
                total_count += int(job_range)
        return total_count

