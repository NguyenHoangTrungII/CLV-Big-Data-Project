import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { HttpClientModule } from '@angular/common/http';

import { AppComponent } from './app.component';
// import { DataDisplayComponent } from './component/data-display/data-display.component';
import { DataService } from './service/data.service';
import { ClvChartComponent } from './component/clv-line-chart/clv-line-chart.component';
import { DashboardComponent } from './component/dashboard/dashboard.component';
import { SocketIoConfig, SocketIoModule } from 'ngx-socket-io';
import { BaseChartDirective } from 'ng2-charts';

const socketConfig: SocketIoConfig = {
  url: 'http://localhost:3000', // Địa chỉ API Flask
  options: {
    transports: ['websocket'],
  },
};
@NgModule({
  declarations: [
    AppComponent,
    ClvChartComponent,
    DashboardComponent
  ],
  imports: [
    BrowserModule,
    HttpClientModule,
    BaseChartDirective,
    SocketIoModule.forRoot(socketConfig)  // Kết nối với backend Flask
    
  ],
  providers: [DataService],
  bootstrap: [AppComponent]
})
export class AppModule { }
