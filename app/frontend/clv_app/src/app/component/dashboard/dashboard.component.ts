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
  // users: { name: string, clv: number }[] = [];  
  users: { name: string, clv: number, rank?: number, change?: 'up' | 'down' | 'none'| 'new' }[] = []; // Thêm thuộc tính change để hiển thị biểu tượng
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
      this.users.push({ name: data.key, clv: data.clv, change: 'new', rank: -1});
    }

    // Sắp xếp danh sách theo CLV giảm dần và chỉ lấy top 10
    // this.users.sort((a, b) => b.clv - a.clv);

    const updatedUsers = [...this.users].sort((a, b) => b.clv - a.clv);
    // Tính toán thứ hạng và cập nhật biểu tượng thay đổi
    updatedUsers.forEach((user, index) => {
    const oldRank = user.rank ?? index;
      user.rank = index; // Cập nhật thứ hạng mới
      if (oldRank === -1) {
        user.change = 'new'; // Nếu là người dùng mới thì đánh dấu 'new'
      } 
      else if (oldRank < index) {
        user.change = 'down'; // Giảm hạng
      } else if (oldRank > index) {
        user.change = 'up'; // Tăng hạng
      } 
      else {
        user.change = 'none'; // Không thay đổi
      }
    });

    // const topUsers = this.users.slice(0, 10);

    const topUsers = updatedUsers.slice(0, 10);

    this.users = updatedUsers.slice(0,5)
    
    console.log('top user', topUsers)
    console.log('user', this.users)


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
    // setTimeout(() => {
    //   const items = document.querySelectorAll('.leaderboard-container li');

    //   console.log("leaderboard-container l", items)
      
    //   items.forEach((item, index) => {
    //     // Gán thêm lớp theo trạng thái của mục
    //     if (this.users[index].change === 'down') {
    //       item.classList.add('down');
    //     } else if (this.users[index].change === 'up') {
    //       item.classList.add('up');
    //     } else {
    //       item.classList.add('newRank');
    //     }
    //     item.classList.add('visible'); // Thêm lớp 'visible' để trượt lên
    //   });
    // }, 0); 

    setTimeout(() => {
      const items = document.querySelectorAll('.leaderboard-container li');
      
      console.log("leaderboard-container li elements:", items); // Kiểm tra xem phần tử có được tìm thấy không
    
      if (items.length === 0) {
        console.log("No leaderboard items found.");
      }
    
      items.forEach((item, index) => {
        item.classList.remove('downRank', 'upRank', 'none', 'newRank', 'visible');

        console.log("item after remove class", item)
        // Kiểm tra nếu index vượt quá số lượng người dùng
        if (this.users[index]) {
          // Gán thêm lớp theo trạng thái của mục
          if (this.users[index].change === 'down') {
            item.classList.add('downRank');
          } else if (this.users[index].change === 'up') {
            item.classList.add('upRank');
          } else if (this.users[index].change === 'none'){
            item.classList.add('none');
          } else{
            item.classList.add('newRank');
          }
          item.classList.add('visible'); // Thêm lớp 'visible' để trượt lên
        }
      });

      const items_after = document.querySelectorAll('.leaderboard-container li');
      
      console.log("leaderboard-container li elements after:", items_after);
    }, 0);
    
  }
  
  

}


