import sys
import itertools
import logging
import math
import copy

logger = logging.getLogger('')

class ClientPlacement:
    WITH_LEADER = 'with_leader'
    BALANCED = 'balanced'


class BalancedPlacementStrategy:
	def generate_layout(self, args, num_c, num_s, num_replicas, hosts_config):
		data_centers = args.data_centers

		self.args = args
		self.num_c = num_c
		self.num_s = num_s
		self.num_replicas = num_replicas

		hosts = self.hosts_by_datacenter(hosts_config['host'].keys(), data_centers)
		server_names = [ 's'+str(i) for i in range(num_s * num_replicas) ]
		client_names = [ 'c'+str(i) for i in range(num_c) ]

		process = {}
		site = {}

		self.generate_process(process, hosts, server_names, client_names)
		self.generate_site(site, server_names, client_names)

		result = {'site': site, 'process': process}
		return result

	def hosts_by_datacenter(self, hosts, data_centers):
		if len(data_centers) == 0:
			return {'': sorted(hosts)}
		else:
			result = {}

			for dc in data_centers:
				result[dc] = []

			for h in hosts:
				for dc in data_centers:
					if h.find(dc)==0:
						result[dc].append(h)
						break

			sorted_result = { dc: sorted(value) for (dc, value) in result.items() }
			return sorted_result

	def generate_process(self, process, hosts, server_names, client_names):
		num_datacenters = len(hosts.keys())
		num_servers = int(math.ceil(float(len(server_names)) / float(self.args.cpu_count)))
		num_servers_per_datacenter = int(math.ceil(float(num_servers) / float(num_datacenters)))

		logging.info("num_datacenters: %s", num_datacenters)
		logging.info("num_servers: %s", num_servers)
		logging.info("num_servers_per_datacenter: %s", num_servers_per_datacenter)

		# partition hosts in to servers and clients
		server_machines = {dc: [] for dc in hosts.keys()}
		for dc in hosts.keys():
			server_machines[dc].extend(hosts[dc][:num_servers_per_datacenter])


		num_client_hosts = 0
		client_machines = {dc: [] for dc in hosts.keys()}
		for dc in hosts.keys():
			server_set = set(server_machines[dc])
			all_set = set(hosts[dc])
			clients = list(all_set - server_set)
			client_machines[dc].extend(clients)
			num_client_hosts += len(clients)

		# now match {server, client} names to their respective hosts

		# returns each data center in a round robin fashion -- forever
		datacenter_it = itertools.cycle(hosts.keys())

		server_hosts_it = {dc: itertools.cycle(hosts) for (dc, hosts) in server_machines.items()}
		for server in server_names:
			dc = next(datacenter_it)
			server_host = next(server_hosts_it[dc])
			logging.info("map %s -> %s", server, server_host)
			process[server] = server_host

		datacenter_it = itertools.cycle(hosts.keys())

		if num_client_hosts == 0:
			if self.args.allow_client_overlap:
				client_machines = server_machines
			else:
				logger.error("no client machines available. exiting program.")
				logger.error("use --allow-client-overlap if testing locally.")
				sys.exit(1)

		client_hosts_it = {dc: itertools.cycle(hosts) for (dc, hosts) in client_machines.items()}
		for client in client_names:
			dc = next(datacenter_it)
			client_host = next(client_hosts_it[dc])
			logging.info("map %s -> %s", client, client_host)
			process[client] = client_host


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
