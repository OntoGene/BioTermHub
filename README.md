# OntoGene Bio Term Hub

## Python Package Structure

Subpackages:
* core: global settings, central resource compiler
* inputfilters: separate modules for each input resource
* lib: helpers
* stats: counting and plotting
* update: fetch up-to-date original-resource dumps
* www: web GUI

## Setup

The directory containing this file must be named "termhub".

For the automatic updater and the web interface to work, cd into the directory containing this file and run `make`.
