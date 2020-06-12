# novafilter

Simulated version of OpenStack Nova's filter based VM scheduler.

## Python virtual environment

We use Python virtual environment ([venv](https://docs.python.org/3/library/venv.html))
for ease of development and portability across different Operating Systems.
We use Python 3.7.0. This is reflected in [.python-version](./.python-version)
file that gets picked up by this virtual environment creation command

```
$ python3 -m venv ~/nf
```

This command will create `nf` directory in your home folder.
We name our virtual environment as `nf` but you are
welcome to use a different name. However, make sure to replace `nf` with the 
venv-name-you-pick in the following command.

Activate venv and confirm that it is running Python 3.7.0
```
$ source ~/nf/bin/activate
(nf) $ python --version
Python 3.7.0
```

You can simply type `$ deactivate` anytime to get out of the `nf` virtual environment.

Finally, run `(nf) $ make install` to get all library dependencies installed.
Now you should be able to run all Python scripts in this repo. If this is not the
case, please fix the errors and update this readme accordingly.
