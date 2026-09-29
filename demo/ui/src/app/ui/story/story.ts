import { DatePipe } from '@angular/common';
import { ChangeDetectionStrategy, Component, inject } from '@angular/core';
import { DemoStore } from '../../core/store';

/** The narrator: plain-language explanations of what the library just did. */
@Component({
  selector: 'app-story',
  imports: [DatePipe],
  templateUrl: './story.html',
  styleUrl: './story.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class Story {
  readonly store = inject(DemoStore);
}
