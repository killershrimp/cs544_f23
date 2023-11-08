#!/usr/bin/env bash

wget -nc https://pages.cs.wisc.edu/~harter/cs544/data/hdma-wi-2021.zip
yes n | unzip hdma-wi-2021.zip
grep -o 'Multifamily' hdma-wi-2021.csv | wc -l
