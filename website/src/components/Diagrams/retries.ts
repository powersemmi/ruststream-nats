import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * A retry on NATS: the runtime reads the attempt count (the server's on JetStream, its own header
 * on Core), and the cap decides. Below it, each model brings the message back its own way; at
 * it, the delivery goes to the dead-letter subject or is ended.
 */
export function retries(words: Words): {boxes: Box[]; links: Link[]} {
  const box = (
    id: string,
    x: number,
    y: number,
    w: number,
    h: number,
    fields: Partial<Box> = {},
  ): Box => ({id, x, y, w, h, kind: 'step', lines: lines(words, id), ...fields});
  const boxes: Box[] = [
    box('delivery', 12, 0, 336, 44, {kind: 'broker'}),
    box('outcome', 12, 72, 336, 44, {kind: 'service', href: '/components/settlement/retries/'}),
    box('cap', 12, 144, 336, 44, {kind: 'core', href: '/components/settlement/retries/'}),
    box('below', 0, 220, 172, 236, {kind: 'group'}),
    box('nak', 12, 246, 148, 58, {kind: 'broker'}),
    box('copy', 12, 316, 148, 58, {kind: 'step'}),
    box('named', 12, 386, 148, 58, {kind: 'step', href: '/components/settlement/out-retry/'}),
    box('atCap', 188, 220, 172, 236, {kind: 'group'}),
    box('deadLetter', 200, 246, 148, 72, {
      kind: 'bad',
      href: '/components/settlement/dead-letters/',
    }),
    box('terminated', 200, 330, 148, 72, {kind: 'bad'}),
    box('lost', 0, 476, 360, 44, {kind: 'note'}),
  ];
  const links: Link[] = [
    {from: 'delivery', to: 'outcome', label: word(words, 'handled')},
    {from: 'outcome', to: 'cap', label: word(words, 'retries')},
    {from: 'cap', to: 'below', exitAt: (86 - 12) / 336, label: word(words, 'belowCap')},
    {from: 'cap', to: 'atCap', exitAt: (274 - 12) / 336, label: word(words, 'reached')},
  ];
  return {boxes, links};
}
