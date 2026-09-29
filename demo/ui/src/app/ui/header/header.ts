import { ChangeDetectionStrategy, Component, inject } from '@angular/core';
import { DemoStore } from '../../core/store';

@Component({
  selector: 'app-header',
  templateUrl: './header.html',
  styleUrl: './header.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class Header {
  readonly store = inject(DemoStore);
}
