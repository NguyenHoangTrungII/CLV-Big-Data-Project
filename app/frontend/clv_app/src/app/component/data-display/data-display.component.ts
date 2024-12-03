import { Component } from '@angular/core';
import { DataService } from '../../service/data.service';

@Component({
  selector: 'app-data-display',
  standalone: false,
  templateUrl: './data-display.component.html',
  styleUrls: ['./data-display.component.scss']
})
export class DataDisplayComponent {
  // Thuộc tính lưu trữ dữ liệu và trạng thái tải
  data: any[] = [];  // Lưu trữ dữ liệu trả về từ API
  isLoading: boolean = true;  // Trạng thái tải dữ liệu

  constructor(private dataService: DataService) {}

  ngOnInit() {
    this.loadData();
  }

  // Hàm để gọi API và lấy dữ liệu
  loadData() {
    this.dataService.getData().subscribe(
      (response) => {
        this.data = response;  // Cập nhật dữ liệu từ API
        this.isLoading = false;  // Đánh dấu là đã tải xong
        console.log(this.data);  // In dữ liệu ra console để kiểm tra
      },
      (error) => {
        console.error('Lỗi khi tải dữ liệu:', error);
        this.isLoading = false;  // Đánh dấu là đã tải xong dù có lỗi
      }
    );
  }
}
