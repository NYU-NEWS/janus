import subprocess
import threading
import logging
import Queue

def ps(hosts, grep_filter):
    output = [] 
    queue = Queue.Queue(len(hosts))

    def work(host, grep_filter):
        cmd = ['/bin/bash', '-c', "'ps -eLF | grep \"{}\"'".format(grep_filter)]
        ssh_cmd = ['ssh', host]
        ssh_cmd.extend(cmd)
        output = ""
        output += "----------\nServer: {}\n-------------\n".format(host)

        try:
            o = subprocess.check_output(ssh_cmd)
            output += o
        except subprocess.CalledProcessError as e:
            output += "error calling ps! returncode {}".format(e.returncode)
        queue.put(output)

   
    threads=[]
    for host in hosts:
        t = threading.Thread(target=work, args=(host, grep_filter,))
        threads.append(t)
        t.start()
    
    for x in range(len(hosts)):
        try:
            output.append(queue.get(True, 1))
        except Queue.Empty as e:
            logging.debug("timeout waiting for value from: " + str(x))
            pass

    return '\n'.join(output)


def killall(hosts, proc, param="-9"):
    def work(host, proc, param):
        cmd = ['killall', '-q', param, proc]
        ssh_cmd = ['ssh', host]
        ssh_cmd.extend(cmd)
        res = subprocess.call(ssh_cmd)
        if res != 0:
            logging.error("host: {}; killall did not kill anything".format(res))

    threads=[]
    for host in hosts:
        t = threading.Thread(target=work,args=(host, proc, param,))
        threads.append(t)
        t.start()
    
    logging.info("waiting for killall commands to finish.")
    for t in threads:
        t.join()
    logging.info("done waiting for killall commands to finish.")

