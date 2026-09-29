import { ChangeDetectionStrategy, Component, inject, signal } from '@angular/core';
import { StreamService } from './core/stream.service';
import { BreakerPanel } from './ui/breaker-panel/breaker-panel';
import { ControlBar } from './ui/control-bar/control-bar';
import { FlowDiagram } from './ui/flow-diagram/flow-diagram';
import { Header } from './ui/header/header';
import { KpiStrip } from './ui/kpi-strip/kpi-strip';
import { PipelinePanel } from './ui/pipeline-panel/pipeline-panel';
import { RequestFeed } from './ui/request-feed/request-feed';
import { Story } from './ui/story/story';
import { ThroughputChart } from './ui/throughput-chart/throughput-chart';

@Component({
  selector: 'app-root',
  imports: [Header, ControlBar, KpiStrip, FlowDiagram, Story, BreakerPanel, ThroughputChart, PipelinePanel, RequestFeed],
  templateUrl: './app.html',
  styleUrl: './app.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class App {
  private readonly stream = inject(StreamService);
  readonly speed = signal(1);

  constructor() {
    this.stream.connect();
  }
}
