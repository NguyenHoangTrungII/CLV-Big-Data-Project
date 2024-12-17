import { Component, OnInit } from '@angular/core';
import { Socket } from 'ngx-socket-io';
import { ChartData, ChartOptions } from 'chart.js';
import { Chart as ChartJS, CategoryScale, LinearScale, BarElement, Title, Tooltip, Legend, BarController } from 'chart.js';

ChartJS.register(CategoryScale, LinearScale, BarElement, Title, Tooltip, Legend, BarController);

@Component({
  selector: 'app-dashboard',
  standalone: false,
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.scss']
})
export class DashboardComponent implements OnInit {
  // Lưu trữ người dùng và CLV của họ
  users: { name: string, clv: number }[] = [];  

  chartData: ChartData<'bar'> = {
    labels: [],  // Các tên người dùng
    datasets: [
      {
        label: 'Top 10 CLV',
        data: [],  // Giá trị CLV tương ứng
        borderColor: 'rgba(75, 192, 192, 1)',
        backgroundColor: 'rgba(75, 192, 192, 0.2)',
        // fill: true
      }
    ]
  };

  chartOptions: ChartOptions = {
    responsive: true,
    scales: {
      x: {
        beginAtZero: true,
        title: {
          display: true,
          text: 'Users'
        }
      },
      y: {
        beginAtZero: true,
        title: {
          display: true,
          text: 'CLV'
        }
      }
    }
  };

  constructor(private socket: Socket) {}

  ngOnInit(): void {
    // Lắng nghe sự kiện 'new_clv_data' từ server (Flask)
    this.socket.fromEvent('new_clv_data').subscribe((data: any) => {
      console.log(data);
      this.updateTopCLV(data);
    });
  }

  // Cập nhật danh sách top 10 CLV
  updateTopCLV(data: any): void {
    // Thêm người dùng mới vào mảng users hoặc cập nhật CLV của họ
    const existingUserIndex = this.users.findIndex(user => user.name === data.key);
    if (existingUserIndex !== -1) {
      // Cập nhật CLV nếu người dùng đã có trong danh sách
      this.users[existingUserIndex].clv = data.clv;
    } else {
      // Thêm người dùng mới vào danh sách
      this.users.push({ name: data.key, clv: data.clv });
    }

    // Sắp xếp danh sách theo CLV giảm dần và chỉ lấy top 10
    this.users.sort((a, b) => b.clv - a.clv);
    const topUsers = this.users.slice(0, 10);

    console.log('top user', topUsers)

    // Cập nhật dữ liệu biểu đồ
    this.chartData = {
      labels: topUsers.map(user => user.name),  // Các tên người dùng
      datasets: [
        {
          label: 'Top 10 CLV',
          data: topUsers.map(user => user.clv),  // Các giá trị CLV tương ứng
          borderColor: 'rgba(75, 192, 192, 1)',
          backgroundColor: 'rgba(75, 192, 192, 0.2)',
          // fill: true
        }
      ]
    };
  }
}
