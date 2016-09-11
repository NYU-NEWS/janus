import sys
import itertools
import logging
import math

logger = logging.getLogger('')

class ClientPlacement:
    WITH_LEADER = 'with_leader'
    BALANCED = 'balanced'

class BalancedPlacementStrategy:
	def generate_layout(self, args, num_c, num_s, num_replicas, hosts_config):
		self.args = args
		self.num_c = num_c
		self.num_s = num_s
		self.num_replicas = num_replicas

		hosts = [ name for name in hosts_config['host'].keys() ]
		server_names = [ 's'+str(i) for i in range(num_s * num_replicas) ]
		client_names = [ 'c'+str(i) for i in range(num_c) ]

		process = {}
		site = {}

		self.generate_process(process, hosts, server_names, client_names)
		self.generate_site(site, server_names, client_names)

		result = {'site': site, 'process': process}
		return result

	def generate_process(self, process, hosts, server_names, client_names):
		def next_host(hosts_it):
			return hosts_it.next()

		# identify server hosts and account for extra cpu
		tot_procs = self.num_s * self.num_replicas
		num_server_machines = int(math.ceil(float(tot_procs) / self.args.cpu_count))
		server_hosts = hosts[:num_server_machines]
		logger.debug("server hosts: %s", ', '.join(server_hosts))
		tmp = list(server_hosts)

		for h in server_hosts:
			# append extra cpu to server host lists
			for x in range(self.args.cpu_count-1):
				tmp.append(h)
		hosts_it = itertools.cycle(tmp)
		server_processes = {name: next_host(hosts_it) for name in server_names}

		remaining_hosts = list(set(hosts) - set(server_hosts))
		if len(remaining_hosts)!=0:
			hosts_it = itertools.cycle(remaining_hosts)
			logger.debug("client hosts: %s", ', '.join(remaining_hosts))

		client_processes = {name: next_host(hosts_it) for name in client_names}
		process.update(client_processes)
		process.update(server_processes)

	def generate_site(self, site, server_names, client_names):
		self.generate_site_server(site, server_names)
		self.generate_site_client(site, client_names)

	def generate_site_server(self, site, server_names):
		site.update({'server': [], 'client': []})
		port = 10000
		for sid in range(self.num_s):
			row = []
			for repid in range(self.num_replicas):
				index = self.num_replicas*sid + repid
				row.append(server_names[index] + ':' + str(port))
				port += 1
			site['server'].append(row)

	def generate_site_client(self, site, client_names):
		row = []
		for cid in range(self.num_c):
			row.append(client_names[cid])
			if len(row) % (self.num_replicas) == 0 and len(row) > 0:
				site['client'].append(row)
				row = []
		if len(row) > 0:
			site['client'].append(row)


class LeaderPlacementStrategy(BalancedPlacementStrategy):
	def generate_site_client(self, site, client_names):
		row = []
		for name in client_names:
			site['client'].append([name])