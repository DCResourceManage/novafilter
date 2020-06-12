.EXPORT_ALL_VARIABLES:

export PYTHONPATH=$(PWD)

install:
	pip install -r requirements.txt

run_tests:
	pytest

all: install run_tests 
