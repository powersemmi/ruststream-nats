import {consumers} from './consumers';
import {design} from './design';
import type {Box, Lane, Link} from './flow';
import {lanes} from './lanes';
import {lifecycle} from './lifecycle';
import {path} from './path';
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
  design,
  lifecycle,
  path,
  lanes,
} satisfies Record<string, (words: Words) => Layout>;

export type DiagramName = keyof typeof LAYOUTS;
