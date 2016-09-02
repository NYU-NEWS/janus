import sys
import itertools

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
		hosts_it = itertools.cycle(hosts)
		server_processes = {name: hosts_it.next() for name in server_names}
		client_processes = {name: hosts_it.next() for name in client_names}		
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