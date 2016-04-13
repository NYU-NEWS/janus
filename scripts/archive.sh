#! /usr/bin/env bash
tar -czvf archive/$1.tgz log/* tmp/*yml config/*yml && rm log/*
