from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ValidationError
from typing import List
from confluent_kafka import Producer
import json
import uvicorn

# Define the FastAPI app
app = FastAPI()

# Kafka configuration
KAFKA_BROKER = "localhost:9093"
KAFKA_TOPIC = "test"

# Configure Kafka Producer
producer = Producer({'bootstrap.servers': KAFKA_BROKER})


# Define the schema for the data using Pydantic
class Item(BaseModel):
    StockCode: str
    Description: str
    Quantity: int
    UnitPrice: float


class Invoice(BaseModel):
    InvoiceNo: str
    InvoiceDate: str
    CustomerID: str
    Country: str
    Items: List[Item]


# Kafka delivery report callback
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


# API endpoint to send invoice data to Kafka
@app.post("/send-invoice/")
async def send_invoice(invoice: Invoice):
    try:
        # Serialize the invoice data to JSON
        invoice_json = invoice.json()
        
        # Send the serialized data to Kafka
        producer.produce(KAFKA_TOPIC, value=invoice_json, callback=delivery_report)
        producer.flush()

        return {"message": "Invoice sent successfully!"}
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=f"Invalid data: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error sending data: {str(e)}")
if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=8000)
