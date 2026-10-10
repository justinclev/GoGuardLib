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

  /**
   * Which build of this page is running: the hash in the name of the script that was loaded.
   * If it does not change after a rebuild, the browser or a container is serving an old copy.
   */
  readonly build =
    document.querySelector('script[src*="main-"]')?.getAttribute('src')?.match(/main-([A-Za-z0-9]+)\.js/)?.[1] ?? 'dev';
}
