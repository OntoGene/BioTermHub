# OntoGene Bio Term Hub

The Bio Term Hub (BTH) is an aggregator of biomedical terminologies sourced from manually curated databases.


## Setup/Installation

To initiate a running instance with a web interface and an automatic updater, a few steps are necessary:

* The directory containing this file must be named "termhub".
  This allows to use it as a Python library (`from termhub import ...`)
* Paths and other configuration parameters must be adapted in [core/settings.py](/core/settings.py).
* Change into the directory containing this file and run `make`.
  This will create empty directories and log files according to the configuration in [core/settings.py](/core/settings.py).


## Python Package Structure

Subpackages:
* core: global settings, central resource compiler
* inputfilters: separate modules for each input resource
* lib: helpers
* stats: counting and plotting
* update: fetch up-to-date original-resource dumps
* www: web GUI


## License

Licensed under the BSD 2-Clause License, see [LICENSE.md](LICENSE.md).
