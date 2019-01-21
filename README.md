# OntoGene Bio Term Hub

The Bio Term Hub (BTH) is an aggregator of biomedical terminologies sourced from manually curated databases.


## Setup/Installation

To initiate a running instance with a web interface and an automatic updater, a few steps are necessary:

* Paths and other configuration parameters must be adapted in [bth/core/settings.py](/bth/core/settings.py).
* Change into the directory containing this file and run `make`.
  This will create empty directories and log files according to the configuration in [settings.py](/bth/core/settings.py).
* In order to include UMLS identifiers (CUIs), run `make umls-cuis`.
  This will download a Bash script to _bth/update/curl-uts-download.sh_.
  Edit this file to include your personal UTS credentials at the top.
  Then execute `./run extract-umls-cuis -f` to download all of UMLS and extract the relevant CUI entries (this will take a while).


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
