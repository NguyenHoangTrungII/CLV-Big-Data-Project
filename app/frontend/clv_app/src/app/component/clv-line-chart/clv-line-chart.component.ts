import { Component, OnInit } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { environment } from '../../environments/environment';
import { Chart, CategoryScale, LinearScale, LineElement, PointElement, Title, Tooltip, Legend, LineController } from 'chart.js';

// Đăng ký các thành phần cần thiết cho biểu đồ
Chart.register(
  CategoryScale,    // Đăng ký category scale cho trục X
  LinearScale,      // Đăng ký linear scale cho trục Y
  LineElement,      // Đăng ký LineElement (cho đường line)
  PointElement,     // Đăng ký PointElement (cho các điểm trên đường line)
  Title,            // Đăng ký Title (cho tiêu đề biểu đồ)
  Tooltip,          // Đăng ký Tooltip (cho các tooltip)
  Legend,           // Đăng ký Legend (cho chú thích)
  LineController    // Đăng ký LineController cho biểu đồ line
);

@Component({
  selector: 'app-clv-line-chart',
  standalone: false,
  templateUrl: './clv-line-chart.component.html',
  styleUrls: ['./clv-line-chart.component.scss']
})
export class ClvChartComponent implements OnInit {
  chart: any;

  constructor(private http: HttpClient) {}

  ngOnInit(): void {
    this.http.get<any[]>('http://localhost:5000/data').subscribe(data => {
      // const labels = data.map(item => item.date);
      // const clvData = data.map(item => item.clv);
      console.log(data)

       // Lấy labels (ngày) và clvData (dự đoán CLV)
      const labels = data.map(item => {
        // Chuyển đổi ngày sang định dạng có thể đọc được
        const date = new Date(item.data['cf:InvoiceDate']);
        return date.toLocaleDateString();  // Bạn có thể thay đổi cách định dạng ngày nếu cần
      });

      const clvData = data.map(item => parseFloat(item.data['cf:CLV_Prediction'])); // Chuyển đổi sang kiểu số

      // Vẽ biểu đồ Line Chart
      this.chart = new Chart('canvas', {
        type: 'line',
        data: {
          labels: labels,  // Ngày
          datasets: [{
            label: 'CLV Prediction',
            data: clvData,  // Dữ liệu CLV
            borderColor: 'rgba(75, 192, 192, 1)', // Màu đường
            backgroundColor: 'rgba(75, 192, 192, 0.2)', // Màu nền
            fill: true,
            tension: 0.4, // Độ mượt của đường
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          scales: {
            x: {
              title: {
                display: true,
                text: 'Ngày'
              },
              type: 'category',  // Xác định scale là 'category' cho trục X
            },
            y: {
              title: {
                display: true,
                text: 'CLV'
              },
              type: 'linear',  // Trục Y là scale tuyến tính
              beginAtZero: true
            }
          }
        }
      });
    });
  }
}
