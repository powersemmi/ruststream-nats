import type {Box, Link} from './flow';
import {chain} from './chain';
import {lines, type Words} from './words';

const COLUMN = 172;
const RIGHT = 188;

/*
 * The path of one message that is answered, for each model: the two arrive differently and
 * settle differently, and between those ends they share one way through the runtime.
 */
export function path(words: Words): {boxes: Box[]; links: Link[]} {
  const side = (
    id: string,
    x: number,
    y: number,
    kind: Box['kind'],
    href?: string,
  ): Box => ({id, x, y, w: COLUMN, h: 58, kind, lines: lines(words, id), href});
  const shared = chain(
    words,
    [
      {key: 'dispatch', kind: 'core', href: '/architecture/lanes/'},
      {key: 'decode'},
      {key: 'body', kind: 'service', href: '/components/subscriptions/'},
      {key: 'encode'},
      {key: 'publisher', kind: 'broker', href: '/components/publishing/'},
    ],
    {top: 180},
  );
  const last = shared.boxes[shared.boxes.length - 1]!;
  const bottom = last.y + last.h + 32;
  const boxes: Box[] = [
    side('coreSource', 0, 0, 'broker', '/architecture/nats/'),
    side('jetStreamSource', RIGHT, 0, 'broker', '/architecture/nats/consumers/'),
    side('coreMessage', 0, 86, 'broker'),
    side('jetStreamMessage', RIGHT, 86, 'broker', '/components/context/'),
    ...shared.boxes,
    side('coreSettle', 0, bottom, 'bad', '/components/settlement/at-most-once/'),
    side('jetStreamSettle', RIGHT, bottom, 'good', '/components/settlement/'),
  ];
  const links: Link[] = [
    {from: 'coreSource', to: 'coreMessage'},
    {from: 'jetStreamSource', to: 'jetStreamMessage'},
    {from: 'coreMessage', to: 'dispatch', enterAt: 0.25},
    {from: 'jetStreamMessage', to: 'dispatch', enterAt: 0.75},
    ...shared.links,
    {from: 'publisher', to: 'coreSettle', exitAt: 0.25},
    {from: 'publisher', to: 'jetStreamSettle', exitAt: 0.75},
  ];
  return {boxes, links};
}
