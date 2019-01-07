# OntoGene Bio Term Hub

The Bio Term Hub (BTH) is an aggregator of biomedical terminologies sourced from manually curated databases.


## Setup/Installation

To initiate a running instance with a web interface and an automatic updater, a few steps are necessary:

* Paths and other configuration parameters must be adapted in [bth/core/settings.py](/bth/core/settings.py).
* Change into the directory containing this file and run `make`.
  This will create empty directories and log files according to the configuration in [settings.py](/bth/core/settings.py).


## Python Package Structure

Subpackages:
* bth.core: global settings, central resource compiler
* bth.inputfilters: separate modules for each input resource
* bth.lib: helpers
* bth.stats: counting and plotting
* bth.update: fetch up-to-date original-resource dumps

The _www_ directory contains a script and HTML template for running a web GUI through CGI.


## License

Licensed under the AGPL-3.0 License, see [LICENSE](LICENSE).
