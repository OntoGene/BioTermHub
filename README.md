# OntoGene Bio Term Hub

The Bio Term Hub (BTH) is an aggregator of biomedical terminologies sourced from manually curated databases.


## Setup/Installation

To initiate a running instance with a web interface and an automatic updater, a few steps are necessary:

* Paths and other configuration parameters must be adapted in [bth/core/settings.py](/bth/core/settings.py).
* Change into the directory containing this file and run `make`.
  This will create empty directories and log files according to the configuration in [settings.py](/bth/core/settings.py).
* The `run` executable provides a command-line interface to the BTH.
  Call it as `./run CMD [OPTIONS]`, where CMD can be "server" or "aggregate", among others.
* To start a web server, call eg. `./run server -i localhost -p 1234`, then point your browser to _http://localhost:1234_.
  Before starting the server for the first time, you should initialise the resource cache with `./run fetch-remote all`.
* _Optional:_ In order to include UMLS identifiers (CUIs), run `make umls-cuis`.
  This will download a Bash script to _bth/update/curl-uts-download.sh_.
  Edit this file to include your personal UTS credentials at the top.
  Then execute `./run extract-umls-cuis -f` to download all of UMLS and extract the relevant CUI entries (this will take a while).


## Python Package Structure

Subpackages:
* bth.core: global settings, central resource compiler
* bth.inputfilters: separate modules for each input resource
* bth.lib: helpers
* bth.server: run the BTH as a web server
* bth.stats: counting and plotting
* bth.update: fetch up-to-date original-resource dumps


## License

Licensed under the AGPL-3.0 License, see [LICENSE](LICENSE).
