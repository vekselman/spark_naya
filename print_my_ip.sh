#!/bin/bash

ip addr | grep 192.168 | cut -d ' ' -f6 | cut -d '/' -f1
