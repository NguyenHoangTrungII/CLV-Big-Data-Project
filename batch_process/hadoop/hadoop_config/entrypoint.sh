#!/bin/bash

# Thay thế các biến môi trường vào trong core-site.xml
sed -i "s|\${HADOOP_NAMENODE_HOST}|${HADOOP_NAMENODE_HOST}|g" /usr/local/hadoop/etc/hadoop/core-site.xml
sed -i "s|\${HADOOP_NAMENODE_PORT}|${HADOOP_NAMENODE_PORT}|g" /usr/local/hadoop/etc/hadoop/core-site.xml

