import subprocess
from subprocess import call

run_app_     = "build/deptran_server"
config_path_ = "config/"

modes_ =       ["tpl_ww",
                "occ",
                "tpl_ww_paxos",
                "occ_paxos",
                "tapir",
                "brq"]
sites_ =       ["1c1s1p",
                "2c2s1p",
                "64c8s1p",
                "3c3s3r1p",
                "64c8s3r1p"]
benchmarks_ =  ["rw",
                "tpccd"]

def run(m, s, b):
    pm = config_path_ + m + ".yml"
    ps = config_path_ + s + ".yml"
    pb = config_path_ + b + ".yml"

    cmd = run_app_ + " -f " + pm + " -f " + ps + " -f " + pb + " -P localhost &> " + m + '-' + s + '-' + b + ".txt"
#    print(cmd)
    r = call(cmd, shell=True)
    if r == 0:
        pass
    else:
        print("failed exec:")
        print(cmd)
        pass
    pass

def main():
    for b in benchmarks_:
        for s in sites_:
            for m in modes_:
                run(m, s, b)
    pass

if __name__ == "__main__":
    main()
