FROM debian:latest

RUN apt update && apt install -y wget python3 python3-pip python3-eccodes

RUN pip install eccodes xarray cfgrib
