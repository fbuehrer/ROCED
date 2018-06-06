# ===============================================================================
#
# Copyright (c) 2010, 2011, 2015 by Georg Fleig, Thomas Hauth and Stephan Riedel
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
from __future__ import unicode_literals

import abc
import logging
from datetime import datetime
from operator import attrgetter


class SiteBrokerBase(object):
    """
    Abstract class for SiteBrokers. SiteBrokers (de-)allocate cloud resources.

    Implementations must inherit from this class.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def decide(self, machineTypes, siteInfo):
        # (dict, list) ->dict
        """
        Main SiteBroker method with the intended behaviour.

        :type machineTypes dictionary  machine type (string)   key
                                       MachineStatus object    value
        :type siteInfo     dictionary  site name (string)      key
                                       SiteInformation object  value

        :returns: dictionary: { siteName:[machineName] }
                  Delta of machines on siteName
                  [siteName][machineName] = 0  -> no change on this site
                  [siteName][machineName] = -3  -> shutdown 3 machines on site
        """

    def applyMaxMachinesPerCycle(self):
        pass


class StupidBroker(SiteBrokerBase):
    """
    This class implements a simple cloud allocation scheme:
    - boot required new machines on the cheapest available cloud site(s)
    - shutdown unneeded machines on the most expensive cloud site(s)
    """

    # the global(!) maximum of cloud instances to run
    # can be used as a fallback while debugging Brokering code     
    def __init__(self, max_instances=1000, shutdown_delay=0):
        self.delayedShutdownTime = None
        self.shutdownDelay = shutdown_delay  # seconds
        self._maxInstances = max_instances
        self.logger = logging.getLogger("Broker")

    @staticmethod
    def modSiteOrders(dict_, siteName, machineName, mod):
        """
        Increases or decreases the machines which should be stopped or started
        :param mod:
        :param machineName:
        :param siteName:
        :type dict_: Dictionary to be manipulated
        """
        if mod == 0:
            return

        if siteName not in dict_:
            dict_[siteName] = dict({machineName: 0})

        if machineName not in dict_[siteName]:
            dict_[siteName][machineName] = 0

        dict_[siteName][machineName] += mod

    def decide(self, machineTypes, siteInfo):
        """Redistribute cloud usage."""
        # TODO: report if not all req can be met
        # TODO: Input not yet complete. Broker has to know where each machine is running. FIX!!!
        machinesToSpawn = dict()

        for (mName, mReq) in machineTypes.items():
            # don't request new machines in case of failure
            if mReq.required is not None:
                delta = mReq.required - mReq.actual
            else:
                delta = 0
                mReq.required = 0
            delta = min(self._maxInstances - mReq.actual, delta)

            self.logger.info("Machine type '%s': %d running, %d needed. Spawning/removing %d." %
                             (mName, mReq.actual, mReq.required, delta))

            if mName in machinesToSpawn:
                machinesToSpawn[mName] += delta
            else:
                machinesToSpawn[mName] = delta

        # machinesToSpawn contains a wishlist of machines. Distribute this to the cloud.
        # Spawn cheap sites first.
        cheapFirst = sorted(siteInfo, key=attrgetter("cost"), reverse=False)
        # Shutdown expensive sites first.
        expensiveFirst = sorted(siteInfo, key=attrgetter("cost"), reverse=True)

        siteOrders = dict()

        # spawn
        for mName, toSpawn in machinesToSpawn.items():
            for site in cheapFirst:
                if toSpawn > 0:
                    if mName in site.supportedMachineTypes:
                        self.modSiteOrders(siteOrders, site.siteName, mName, toSpawn)

                        # TODO implement max quota [Siteinfo.maxMachines]
                        toSpawn = 0

        # shutdown
	if toSpawn < 0:
	    return siteOrders #Never shut down machines / remove from queue. not working right now!!!
        for mName, toSpawn in machinesToSpawn.items():
            for site in expensiveFirst:
                if toSpawn < 0:
                    if mName in site.supportedMachineTypes:
                        if self.delayedShutdownTime is not None:
                            if ((datetime.now() - self.delayedShutdownTime).total_seconds() >
                                    self.shutdownDelay):
                                self.modSiteOrders(siteOrders, site.siteName, mName, toSpawn)
                                self.delayedShutdownTime = None
                        else:
                            if self.shutdownDelay == 0:
                                # remove without delay
                                self.modSiteOrders(siteOrders, site.siteName, mName, toSpawn)
                            else:
                                self.delayedShutdownTime = datetime.now()

                        toSpawn = 0

        return siteOrders
