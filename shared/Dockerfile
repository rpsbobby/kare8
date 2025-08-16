FROM python:3.10-slim

WORKDIR /shared

COPY . .

RUN pip install --no-cache-dir -r requirements.txt \
&& mkdir -p /opt/pythonlibs \
&& cp -r messaging_interfaces /opt/pythonlibs \
&& cp -r utils /opt/pythonlibs \
&& cp -r topics /opt/pythonlibs \
&& cp -r entities /opt/pythonlibs \
&& cp -r workers /opt/pythonlibs \
&& cp -r handlers /opt/pythonlibs \
&& chmod -R a+r /opt/pythonlibs

ENV PYTHONPATH="/opt/pythonlibs"
