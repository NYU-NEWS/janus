import subprocess

def ps(hosts, grep_filter):
    output = [] 
    cmd = ['/bin/bash', '-c', "'ps -eLF | grep \"{}\"'".format(grep_filter)]
    for host in hosts:
        ssh_cmd = ['ssh', host]
        ssh_cmd.extend(cmd)
        output.append("----------\nServer: {}\n-------------\n".format(host))
        try:
            o = subprocess.check_output(ssh_cmd)
            output.append(o)
        except subprocess.CalledProcessError as e:
            output.append("error calling ps! returncode {}".format(e.returncode))

    return '\n'.join(output)

