import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * Where the crate sits: the service on top, the core's runtime under it, this crate between the
 * runtime and the async-nats client, and the server at the bottom.
 */
export function design(words: Words): {boxes: Box[]; links: Link[]} {
  const box = (
    id: string,
    x: number,
    y: number,
    w: number,
    h: number,
    fields: Partial<Box> = {},
  ): Box => ({id, x, y, w, h, kind: 'step', lines: lines(words, id), ...fields});
  // The crate's columns leave a gutter at each side, where a link from a box to the client under
  // it runs past the boxes in between.
  const left = 28;
  const right = 182;
  const column = 150;
  const centre = (x: number): number => (x + column / 2 - 12) / 336;
  // Every subscription and every publisher reaches the server through the one client below the
  // crate; the JetStream context is a view of that client, so it sits inside the same frame.
  const boxes: Box[] = [
    box('service', 0, 0, 360, 80, {kind: 'group'}),
    box('handlers', 12, 26, 160, 42, {kind: 'service', code: true}),
    box('app', 188, 26, 160, 42, {kind: 'service', code: true}),
    box('core', 0, 104, 360, 72, {kind: 'group'}),
    box('runtime', 12, 128, 336, 36, {kind: 'core'}),
    box('crate', 0, 200, 360, 204, {kind: 'group'}),
    box('subscriber', left, 226, column, 58, {
      kind: 'broker',
      code: true,
      href: '/architecture/path/',
    }),
    box('publishers', right, 226, column, 58, {
      kind: 'broker',
      code: true,
      href: '/components/publishing/',
    }),
    box('context', left, 294, column, 30, {kind: 'broker', href: '/components/context/'}),
    box('inProcess', right, 294, column, 30, {kind: 'broker', href: '/components/testing/'}),
    box('broker', left, 334, column, 58, {kind: 'broker', code: true, href: '/components/broker/'}),
    box('descriptors', right, 334, column, 58, {
      kind: 'broker',
      code: true,
      href: '/components/subscriptions/',
    }),
    box('client', 0, 428, 360, 116, {kind: 'group'}),
    box('jetstream', left, 454, 304, 36, {
      kind: 'broker',
      code: true,
      href: '/architecture/nats/consumers/',
    }),
    box('connection', left, 498, 304, 36, {kind: 'broker', code: true}),
    box('server', 12, 568, 336, 36, {kind: 'broker'}),
  ];
  const links: Link[] = [
    {from: 'handlers', to: 'runtime', enterAt: 80 / 336, label: word(words, 'mounted')},
    {from: 'runtime', to: 'subscriber', exitAt: centre(left), label: word(words, 'polls')},
    {from: 'subscriber', to: 'connection', exit: 'left', enter: 'left'},
    {from: 'publishers', to: 'connection', exit: 'right', enter: 'right'},
    {from: 'connection', to: 'server', exitAt: 0.5, enterAt: 0.5},
  ];
  return {boxes, links};
}
