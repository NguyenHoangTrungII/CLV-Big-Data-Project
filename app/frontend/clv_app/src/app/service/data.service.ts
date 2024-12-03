import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { environment } from '../environments/environment';

@Injectable({
  providedIn: 'root'
})
export class DataService {

  private apiUrl = environment.apiUrl;  
  constructor(private http: HttpClient) {}

  // Hàm gọi API để lấy dữ liệu
  getData(): Observable<any> {
    return this.http.get<any>(this.apiUrl);
  }
}
