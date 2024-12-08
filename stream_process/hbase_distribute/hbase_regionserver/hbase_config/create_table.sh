#!/bin/bash
echo "Creating table in HBase..."
echo "create 'clv_predictions', 'cf'" | hbase shell
