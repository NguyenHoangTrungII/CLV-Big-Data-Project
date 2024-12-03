export interface DataModel {
    row_key: string;
    data: {
      InvoiceDate: string;
      Quantity: number;
      Revenue: number;
      CLV_Prediction?: number;
    };
  }
  