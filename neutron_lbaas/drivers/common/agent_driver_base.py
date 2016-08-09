# Copyright 2015 Rackspace.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from neutron.common import exceptions as n_exc
from neutron.common import rpc as n_rpc
from neutron.db import agents_db
from neutron.services import provider_configuration as provconf
from oslo_config import cfg
from oslo_log import log as logging
import oslo_messaging as messaging
from oslo_utils import importutils

from neutron_lbaas.drivers.common import agent_callbacks
from neutron_lbaas.drivers import driver_base
from neutron_lbaas.extensions import lbaas_agentschedulerv2
from neutron_lbaas.services.loadbalancer import constants as lb_const
from neutron_lbaas.services.loadbalancer import data_models
from neutron import context
from neutron.openstack.common import loopingcall

LOG = logging.getLogger(__name__)

LB_SCHEDULERS = 'loadbalancer_schedulers'

AGENT_SCHEDULER_OPTS = [
    cfg.StrOpt('loadbalancer_scheduler_driver',
               default='neutron_lbaas.agent_scheduler.ChanceScheduler',
               help=_('Driver to use for scheduling '
                      'to a default loadbalancer agent')),
    cfg.IntOpt('lbaas_agent_monitoring_interval',
               default=10,
               help=_('Seconds between inspecting LBaaS agents states '
                      'in order to find down agents and reschedule '
                      'loadbalancer instances to active agents, if any. '
                      'Zero value means no agent monitoring')),
]

cfg.CONF.register_opts(AGENT_SCHEDULER_OPTS)


class DriverNotSpecified(n_exc.NeutronException):
    message = _("Device driver for agent should be specified "
                "in plugin driver.")


class DataModelSerializer(object):

    def serialize_entity(self, ctx, entity):
        if isinstance(entity, data_models.BaseDataModel):
            return entity.to_dict(stats=False)
        else:
            return entity


class LoadBalancerAgentApi(object):
    """Plugin side of plugin to agent RPC API."""

    # history
    #   1.0 Initial version
    #

    def __init__(self, topic):
        target = messaging.Target(topic=topic, version='1.0')
        self.client = n_rpc.get_client(target,
                                       serializer=DataModelSerializer())

    def agent_updated(self, context, admin_state_up, host):
        cctxt = self.client.prepare(server=host)
        cctxt.cast(context, 'agent_updated',
                   payload={'admin_state_up': admin_state_up})

    def instance_added(self, context, loadbalancer_id, host):
        cctxt = self.client.prepare(server=host)
        cctxt.cast(context, 'instance_added',
                   loadbalancer_id=loadbalancer_id)

    def instance_removed(self, context, loadbalancer_id, host):
        cctxt = self.client.prepare(server=host)
        cctxt.cast(context, 'instance_removed',
                   loadbalancer_id=loadbalancer_id)

    def create_loadbalancer(self, context, loadbalancer, host, driver_name):
        cctxt = self.client.prepare(server=host)
        cctxt.cast(context, 'create_loadbalancer',
                   loadbalancer=loadbalancer, driver_name=driver_name)

    def update_loadbalancer(self, context, old_loadbalancer,
                            loadbalancer, host):
        cctxt = self.client.prepare(server=host)
        cctxt.cast(context, 'update_loadbalancer',
                   old_loadbalancer=old_loadbalancer,
                   loadbalancer=loadbalancer)

    def delete_loadbalancer(self, context, loadbalancer, host):
        cctxt = self.client.prepare(server=host)
        cctxt.cast(context, 'delete_loadbalancer', loadbalancer=loadbalancer)

    def create_listener(self, context, listener, host):
        cctxt = self.client.prepare(server=host)
        cctxt.cast(context, 'create_listener', listener=listener)

    def update_listener(self, context, old_listener, listener,
                        host):
        cctxt = self.client.prepare(server=host)
        cctxt.cast(context, 'update_listener', old_listener=old_listener,
                   listener=listener)

    def delete_listener(self, context, listener, host):
        cctxt = self.client.prepare(server=host)
        cctxt.cast(context, 'delete_listener', listener=listener)

    def create_pool(self, context, pool, host):
        cctxt = self.client.prepare(server=host)
        cctxt.cast(context, 'create_pool', pool=pool)

    def update_pool(self, context, old_pool, pool, host):
        cctxt = self.client.prepare(server=host)
        cctxt.cast(context, 'update_pool', old_pool=old_pool, pool=pool)

    def delete_pool(self, context, pool, host):
        cctxt = self.client.prepare(server=host)
        cctxt.cast(context, 'delete_pool', pool=pool)

    def create_member(self, context, member, host):
        cctxt = self.client.prepare(server=host)
        cctxt.cast(context, 'create_member', member=member)

    def update_member(self, context, old_member, member, host):
        cctxt = self.client.prepare(server=host)
        cctxt.cast(context, 'update_member', old_member=old_member,
                   member=member)

    def delete_member(self, context, member, host):
        cctxt = self.client.prepare(server=host)
        cctxt.cast(context, 'delete_member', member=member)

    def create_healthmonitor(self, context, healthmonitor, host):
        cctxt = self.client.prepare(server=host)
        cctxt.cast(context, 'create_healthmonitor',
                   healthmonitor=healthmonitor)

    def update_healthmonitor(self, context, old_healthmonitor,
                             healthmonitor, host):
        cctxt = self.client.prepare(server=host)
        cctxt.cast(context, 'update_healthmonitor',
                   old_healthmonitor=old_healthmonitor,
                   healthmonitor=healthmonitor)

    def delete_healthmonitor(self, context, healthmonitor, host):
        cctxt = self.client.prepare(server=host)
        cctxt.cast(context, 'delete_healthmonitor',
                   healthmonitor=healthmonitor)


class LoadBalancerManager(driver_base.BaseLoadBalancerManager):

    def update(self, context, old_loadbalancer, loadbalancer):
        super(LoadBalancerManager, self).update(context, old_loadbalancer,
                                                loadbalancer)
        agent = self.driver.get_loadbalancer_agent(context, loadbalancer.id)
        self.driver.agent_rpc.update_loadbalancer(
            context, old_loadbalancer, loadbalancer, agent['host'])

    def create(self, context, loadbalancer):
        super(LoadBalancerManager, self).create(context, loadbalancer)
        agent = self.driver.loadbalancer_scheduler.schedule(
            self.driver.plugin, context, loadbalancer,
            self.driver.device_driver)
        if not agent:
            raise lbaas_agentschedulerv2.NoEligibleLbaasAgent(
                loadbalancer_id=loadbalancer.id)
        self.driver.agent_rpc.create_loadbalancer(
            context, loadbalancer, agent['host'], self.driver.device_driver)

    def delete(self, context, loadbalancer):
        super(LoadBalancerManager, self).delete(context, loadbalancer)
        agent = self.driver.get_loadbalancer_agent(context, loadbalancer.id)
        # TODO(blogan): Rethink deleting from the database here. May want to
        # wait until the agent actually deletes it.  Doing this now to keep
        # what v1 had.
        self.driver.plugin.db.delete_loadbalancer(context, loadbalancer.id)
        if agent:
            self.driver.agent_rpc.delete_loadbalancer(context, loadbalancer,
                                                      agent['host'])

    def stats(self, context, loadbalancer):
        pass

    def refresh(self, context, loadbalancer):
        pass


class ListenerManager(driver_base.BaseListenerManager):

    def update(self, context, old_listener, listener):
        super(ListenerManager, self).update(
            context, old_listener.to_dict(), listener.to_dict())
        agent = self.driver.get_loadbalancer_agent(
            context, listener.loadbalancer.id)
        self.driver.agent_rpc.update_listener(context, old_listener, listener,
                                              agent['host'])

    def create(self, context, listener):
        super(ListenerManager, self).create(context, listener)
        agent = self.driver.get_loadbalancer_agent(
            context, listener.loadbalancer.id)
        self.driver.agent_rpc.create_listener(context, listener, agent['host'])

    def delete(self, context, listener):
        super(ListenerManager, self).delete(context, listener)
        agent = self.driver.get_loadbalancer_agent(context,
                                                   listener.loadbalancer.id)
        # TODO(blogan): Rethink deleting from the database and updating the lb
        # status here. May want to wait until the agent actually deletes it.
        # Doing this now to keep what v1 had.
        self.driver.plugin.db.delete_listener(context, listener.id)
        self.driver.plugin.db.update_loadbalancer_provisioning_status(
            context, listener.loadbalancer.id)
        self.driver.agent_rpc.delete_listener(context, listener, agent['host'])


class PoolManager(driver_base.BasePoolManager):

    def update(self, context, old_pool, pool):
        super(PoolManager, self).update(context, old_pool, pool)
        agent = self.driver.get_loadbalancer_agent(
            context, pool.listener.loadbalancer.id)
        self.driver.agent_rpc.update_pool(context, old_pool, pool,
                                          agent['host'])

    def create(self, context, pool):
        super(PoolManager, self).delete(context, pool)
        agent = self.driver.get_loadbalancer_agent(
            context, pool.listener.loadbalancer.id)
        self.driver.agent_rpc.create_pool(context, pool, agent['host'])

    def delete(self, context, pool):
        super(PoolManager, self).delete(context, pool)
        agent = self.driver.get_loadbalancer_agent(
            context, pool.listener.loadbalancer.id)
        # TODO(blogan): Rethink deleting from the database and updating the lb
        # status here. May want to wait until the agent actually deletes it.
        # Doing this now to keep what v1 had.
        self.driver.plugin.db.delete_pool(context, pool.id)
        self.driver.plugin.db.update_loadbalancer_provisioning_status(
            context, pool.listener.loadbalancer.id)
        self.driver.agent_rpc.delete_pool(context, pool, agent['host'])


class MemberManager(driver_base.BaseMemberManager):

    def update(self, context, old_member, member):
        super(MemberManager, self).update(context, old_member, member)
        agent = self.driver.get_loadbalancer_agent(
            context, member.pool.listener.loadbalancer.id)
        self.driver.agent_rpc.update_member(context, old_member, member,
                                            agent['host'])

    def create(self, context, member):
        super(MemberManager, self).create(context, member)
        agent = self.driver.get_loadbalancer_agent(
            context, member.pool.listener.loadbalancer.id)
        self.driver.agent_rpc.create_member(context, member, agent['host'])

    def delete(self, context, member):
        super(MemberManager, self).delete(context, member)
        agent = self.driver.get_loadbalancer_agent(
            context, member.pool.listener.loadbalancer.id)
        # TODO(blogan): Rethink deleting from the database and updating the lb
        # status here. May want to wait until the agent actually deletes it.
        # Doing this now to keep what v1 had.
        self.driver.plugin.db.delete_pool_member(context, member.id)
        self.driver.plugin.db.update_loadbalancer_provisioning_status(
            context, member.pool.listener.loadbalancer.id)
        self.driver.agent_rpc.delete_member(context, member, agent['host'])


class HealthMonitorManager(driver_base.BaseHealthMonitorManager):

    def update(self, context, old_healthmonitor, healthmonitor):
        super(HealthMonitorManager, self).update(
            context, old_healthmonitor, healthmonitor)
        agent = self.driver.get_loadbalancer_agent(
            context, healthmonitor.pool.listener.loadbalancer.id)
        self.driver.agent_rpc.update_healthmonitor(
            context, old_healthmonitor, healthmonitor, agent['host'])

    def create(self, context, healthmonitor):
        super(HealthMonitorManager, self).create(context, healthmonitor)
        agent = self.driver.get_loadbalancer_agent(
            context, healthmonitor.pool.listener.loadbalancer.id)
        self.driver.agent_rpc.create_healthmonitor(
            context, healthmonitor, agent['host'])

    def delete(self, context, healthmonitor):
        super(HealthMonitorManager, self).delete(context, healthmonitor)
        agent = self.driver.get_loadbalancer_agent(
            context, healthmonitor.pool.listener.loadbalancer.id)
        # TODO(blogan): Rethink deleting from the database and updating the lb
        # status here. May want to wait until the agent actually deletes it.
        # Doing this now to keep what v1 had.
        self.driver.plugin.db.delete_healthmonitor(context, healthmonitor.id)
        self.driver.plugin.db.update_loadbalancer_provisioning_status(
            context, healthmonitor.pool.listener.loadbalancer.id)
        self.driver.agent_rpc.delete_healthmonitor(
            context, healthmonitor, agent['host'])


class AgentDriverBase(driver_base.LoadBalancerBaseDriver):

    # name of device driver that should be used by the agent;
    # vendor specific plugin drivers must override it;
    device_driver = None

    def __init__(self, plugin):
        super(AgentDriverBase, self).__init__(plugin)
        if not self.device_driver:
            raise DriverNotSpecified()

        self.load_balancer = LoadBalancerManager(self)
        self.listener = ListenerManager(self)
        self.pool = PoolManager(self)
        self.member = MemberManager(self)
        self.health_monitor = HealthMonitorManager(self)

        self.agent_rpc = LoadBalancerAgentApi(lb_const.LOADBALANCER_AGENTV2)

        self._set_callbacks_on_plugin()
        # Setting this on the db because the plugin no longer inherts from
        # database classes, the db does.
        self.plugin.db.agent_notifiers.update(
            {lb_const.AGENT_TYPE_LOADBALANCERV2: self.agent_rpc})

        lb_sched_driver = provconf.get_provider_driver_class(
            cfg.CONF.loadbalancer_scheduler_driver, LB_SCHEDULERS)
        self.loadbalancer_scheduler = importutils.import_object(
            lb_sched_driver)

        self._setup_agent_monitoring_on_plugin()

    def _setup_agent_monitoring_on_plugin(self):
        # check if agent monitoring was already set by other plugin driver
        if hasattr(self.plugin, 'agent_monitoring'):
            return
        self.plugin.db.active_agents = set()
        interval = cfg.CONF.lbaas_agent_monitoring_interval
        if interval:
            self.plugin.db.agent_monitoring = (
                loopingcall.FixedIntervalLoopingCall(
                    self._check_agents_states)
            )
            self.plugin.db.agent_monitoring.start(interval=interval,
                                               initial_delay=interval)

    def _check_agents_states(self):
        """Check agents states and reschedule loadbalancers if needed."""
        ctx = context.get_admin_context()
        agents = self.plugin.db.get_lbaas_agents(ctx)
        for agent in agents:
            active = self.plugin.db.is_eligible_agent(True, agent)
            if not active and agent['id'] in self.plugin.db.active_agents:
                self.plugin.db.active_agents.remove(agent['id'])
                # reschedule all instances from that agent to other agents
                LOG.warning(_('LBaaS agent %(agent_id)s on host %(host)s '
                              'became inactive; going to reschedule instances '
                              'to other eligible agents'),
                            {'agent_id': agent['id'], 'host': agent['host']})
                self._reschedule_instances(ctx, agent)
            elif active and agent['id'] not in self.plugin.db.active_agents:
                self.plugin.db.active_agents.add(agent['id'])

    def _reschedule_instances(self, context, agent):
        """Reschedule all loadbalancer instances currently hosted by the agent.
                For each instance which is not host specific, if there is other agent
                able to host it - reschedule to that agent
                """
        loadbalancers = self.plugin.db.list_loadbalancers_on_lbaas_agent(context, agent['id'])
        for loadbalancer in loadbalancers:
            new_agent = self.loadbalancer_scheduler.reschedule(
                self.plugin.db, context, loadbalancer['id'], self.device_driver)
            if new_agent:
                self.agent_rpc.instance_added(
                    context, loadbalancer['id'], new_agent['host'])
                # if agent is back online - it will be notified that no
                # longer handles the instance
                self.agent_rpc.instance_removed(
                    context, loadbalancer['id'], agent['host'])
            else:
                LOG.warning(_('Can\'t reschedule instance %s: no eligible '
                              'agent found'), loadbalancer['id'])

    def _set_callbacks_on_plugin(self):
        # other agent based plugin driver might already set callbacks on plugin
        if hasattr(self.plugin, 'agent_callbacks'):
            return

        self.plugin.agent_endpoints = [
            agent_callbacks.LoadBalancerCallbacks(self.plugin),
            agents_db.AgentExtRpcCallback(self.plugin.db)
        ]
        self.plugin.conn = n_rpc.create_connection(new=True)
        self.plugin.conn.create_consumer(
            lb_const.LOADBALANCER_PLUGINV2,
            self.plugin.agent_endpoints,
            fanout=False)
        self.plugin.conn.consume_in_threads()

    def get_loadbalancer_agent(self, context, loadbalancer_id):
        agent = self.plugin.db.get_agent_hosting_loadbalancer(
            context, loadbalancer_id)
        if not agent:
            raise lbaas_agentschedulerv2.NoActiveLbaasAgent(
                loadbalancer_id=loadbalancer_id)
        return agent['agent']
