import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ClvLineChartComponent } from './clv-line-chart.component';

describe('ClvLineChartComponent', () => {
  let component: ClvLineChartComponent;
  let fixture: ComponentFixture<ClvLineChartComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ClvLineChartComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(ClvLineChartComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
