## Warning
The project has upgraded to python3, so this page is deprecated and needs to be updated. 

## Build on an outdated Linux distribution

Sometimes you need to build on a testbed which are equipped with older systems such as Ubuntu 14.04. You can try Linuxbrew to install newer packages such as boost and yaml-cpp.

Remember to add the following to ~/.bash_profile (or ~/.bashrc if it does not work)
```
export PATH="/home/shuai/.linuxbrew/bin:$PATH"
export MANPATH="/home/shuai/.linuxbrew/share/man:$MANPATH"
export INFOPATH="/home/shuai/.linuxbrew/share/info:$INFOPATH"
export LD_LIBRARY_PATH="/home/shuai/.linuxbrew/lib:$LD_LIBRARY_PATH"
```

Install the needed package
```
brew install python
brew install boost
brew install yaml-cpp
brew install openssh
brew install gperftools
```

Compile and run tests
```
./waf configure build -t
./test_run.py
```

## Select python version on newer systems

This is needed if your default python is python3 (on Arch Linux or some other fancy distros). We need to switch the default python to python2. This can be done via pyenv and virtualenv. (This is a bit tricky. pyenv alone cannot do it because it does not support "--enable-framework" flag on Linux systems)

First, install virtualenv and pyenv (with virtualenv support) For example, on Arch, this can be done by:
```
sudo pip install virtualenv
yaourt -S pyenv pyenv-virtualenv
```

(Don't forget to add to $PATH of pyenv directory)

Then, create a virtualenv and select it as the local python for janus. (make sure you are in the janus directory)

```
pyenv virtualenv -p /usr/bin/python2 venv2
pyenv local venv2
```
