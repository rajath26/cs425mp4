#!/bin/bash


gcc ./src/host.c -o ./src/host `pkg-config --cflags --libs glib-2.0` -lpthread 

if [ $? -eq 0 ]
then 
    echo -e "\nServer compiled\n"
fi

gcc ./src/KVclient.c -o ./src/KVclient `pkg-config --cflags --libs glib-2.0` -lpthread
if [ $? -eq 0 ]
then
    echo -e "\nClient compiled\n"
fi



