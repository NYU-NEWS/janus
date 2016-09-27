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
			return {'': hosts}
		else:
			result = {}

			for dc in data_centers:
				result[dc] = []

			for h in hosts:
				for dc in data_centers:
					if h.find(dc)==0:
						result[dc].append(h)
						break
			return result

	def generate_process(self, process, hosts, server_names, client_names):
		# identify server hosts and account for extra cpu
		tot_procs = self.num_s * self.num_replicas
		num_server_machines = int(math.ceil(float(tot_procs) / self.args.cpu_count))
		data_centers = hosts.keys()
		num_c = len(client_names)
		servers_per_datacenter = num_server_machines / len(data_centers)
		
		server_hosts = {}
		client_hosts = {}
		for dc in data_centers:
			server_hosts[dc] = hosts[dc][0:servers_per_datacenter]
			client_hosts[dc] = hosts[dc][servers_per_datacenter:]

		# append extra cpu to server host lists
		for dc in data_centers:
			hosts = copy.copy(server_hosts[dc])
			for h in hosts:
				for x in range(self.args.cpu_count-1):
					server_hosts[dc].append(h)

		logger.debug("server hosts: %s", server_hosts)

		# map servers to logical names
		host_lists = zip(*[ hosts for hosts in server_hosts.values() ])
		server_num = 0
		server_processes = {}
		for l in host_lists:
			for h in l:
				server_key = 's' + str(server_num)
				server_num += 1
				logger.debug("map {} to {}".format(server_key, h))
				server_processes[server_key] = h

		# map clients to logical names
		host_lists = zip(*[ hosts for hosts in client_hosts.values() ])
		client_num = 0
		client_processes = {}
		for l in host_lists:
			for h in l:
				client_key = 'c' + str(client_num)
				client_num += 1
				logger.debug("map {} to {}".format(client_key, h))
				client_processes[client_key] = h
				if client_num == num_c:
					break
			if client_num == num_c:
				break

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