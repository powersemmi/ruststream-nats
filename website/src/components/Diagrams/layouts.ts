import {consumers} from './consumers';
import type {Box, Lane, Link} from './flow';
import {streams} from './streams';
import {subjects} from './subjects';
import type {Words} from './words';

export interface Layout {
  boxes: Box[];
  links: Link[];
  lanes?: Lane[];
}

/** Every diagram a page can name: a layout places the boxes, the page supplies the words. */
export const LAYOUTS = {
  subjects,
  streams,
  consumers,
} satisfies Record<string, (words: Words) => Layout>;

export type DiagramName = keyof typeof LAYOUTS;
