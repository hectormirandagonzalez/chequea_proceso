#!/bin/sh

GUNICORN=/usr/bin/gunicorn
ROOT=/usr/local/share/api
PID=/var/run/gunicorn.pid
BIND=0.0.0.0:8000
APP=app:app
LOG_ERROR=/var/log/gu-error.log
LOG_ACCESS=/var/log/gu-access.log

if [ -f $PID ]; then rm $PID; fi

cd $ROOT
exec $GUNICORN -b $BIND -c $ROOT/app.py --pid=$PID --access-logfile $LOG_ACCESS --error-logfile $LOG_ERROR $APP
