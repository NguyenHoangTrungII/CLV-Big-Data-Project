import happybase

def connect_to_hbase():
    try:
        # Connect to HBase, adjust the host and port as needed
        connection = happybase.Connection('localhost')  # 'localhost' or your HBase master node IP
        connection.open()
        print("Connected to HBase successfully.")
        return connection
    except Exception as e:
        print(f"Error connecting to HBase: {e}")
        return None

def insert_data_to_hbase(connection, df, table_name='clv_predictions'):
    table = connection.table(table_name)
    
    for _, row in df.iterrows():
        # Create a unique row key (e.g., using 'InvoiceNo' or a combination of features)
        row_key = f"{row['InvoiceNo']}-{row['CustomerID']}"
        
        # Prepare data for HBase
        # Assuming the column family is 'cf' and we are storing the 'CLV_Prediction'
        data = {
            'cf:Quantity': str(row['Quantity']).encode('utf-8'),  # Convert to binary (utf-8 encoding)
            'cf:UnitPrice': str(row['UnitPrice']).encode('utf-8'),  # Convert to binary
            'cf:InvoiceDate': row['InvoiceDate'].isoformat().encode('utf-8'),  # Convert timestamp to string and then to binary
            'cf:hour': str(row['hour']).encode('utf-8'),  # Convert to binary
            'cf:dayofweek': str(row['dayofweek']).encode('utf-8'),  # Convert to binary
            'cf:weekend': str(row['weekend']).encode('utf-8'),  # Convert to binary
            'cf:Revenue': str(row['Revenue']).encode('utf-8'),  # Convert to binary
            'cf:CLV_Prediction': str(row['CLV_Prediction']).encode('utf-8')  # Convert prediction to binary
        }

        try:
            table.put(row_key, data)
            print(f"Inserted data for InvoiceNo {row['InvoiceNo']}")
        except Exception as e:
            print(f"Error inserting data for InvoiceNo {row['InvoiceNo']}: {e}")
