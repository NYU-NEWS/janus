import glob
import yaml

files = glob.glob('*.yml')
for fn in files:
    y = None
    print "file: {}".format(fn)
    with open(fn, 'r') as f:
        y = yaml.load(f)
        for key in y:
            y[key]['zipf']=0.0
    with open(fn, 'w') as f:
        yaml.dump(y, f)
