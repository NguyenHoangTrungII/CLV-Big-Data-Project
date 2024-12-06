import { Component, OnInit } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { Socket } from 'ngx-socket-io';

@Component({
  selector: 'app-root',
  standalone: false,
  // imports: [RouterOutlet],
  templateUrl: './app.component.html',
  styleUrl: './app.component.scss'
})
export class AppComponent implements OnInit {

  constructor(private socket: Socket) {}

  ngOnInit() {
    // Lắng nghe sự kiện
    this.socket.on('message', (data: any) => {
      console.log('Received message: ', data);
    });

    // Gửi dữ liệu
    this.socket.emit('event', { data: 'example' });
  }
}
