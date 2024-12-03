import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { HttpClientModule } from '@angular/common/http';

import { AppComponent } from './app.component';
import { DataDisplayComponent } from './component/data-display/data-display.component';
import { DataService } from './service/data.service';

@NgModule({
  declarations: [
    AppComponent,
    DataDisplayComponent
  ],
  imports: [
    BrowserModule,
    HttpClientModule,
    
  ],
  providers: [DataService],
  bootstrap: [AppComponent]
})
export class AppModule { }
