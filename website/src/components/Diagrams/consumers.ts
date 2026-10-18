import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

const COLUMN = 112;
const COLUMNS = [0, 124, 248] as const;

/*
 * A JetStream pull consumer: the client asks for a batch, the consumer hands out messages from
 * its position in the stream, and the client answers each one; a message nobody answers comes
 * back once ack_wait runs out.
 */
export function consumers(words: Words): {boxes: Box[]; links: Link[]} {
  const answer = (id: string, column: number, kind: Box['kind'], href: string): Box => ({
    id,
    x: COLUMNS[column]!,
    y: 296,
    w: COLUMN,
    h: 74,
    kind,
    lines: lines(words, id),
    href,
  });
  const boxes: Box[] = [
    {id: 'stream', x: 12, y: 0, w: 336, h: 36, kind: 'broker', lines: lines(words, 'stream')},
    {
      id: 'consumer',
      x: 12,
      y: 64,
      w: 336,
      h: 74,
      kind: 'broker',
      lines: lines(words, 'consumer'),
      href: '/components/subscriptions/jetstream-subject/',
    },
    {id: 'client', x: 12, y: 210, w: 336, h: 44, kind: 'service', lines: lines(words, 'client')},
    answer('ack', 0, 'good', '/components/settlement/'),
    answer('nak', 1, 'step', '/components/settlement/retry-after/'),
    answer('term', 2, 'bad', '/components/settlement/'),
    {
      id: 'redelivery',
      x: 0,
      y: 394,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'redelivery'),
      href: '/components/settlement/ack-wait/',
    },
  ];
  // A point on the wide boxes straight above a column's centre.
  const above = (column: number): number => (COLUMNS[column]! + COLUMN / 2 - 12) / 336;
  const links: Link[] = [
    {from: 'stream', to: 'consumer'},
    {
      from: 'consumer',
      to: 'client',
      exitAt: above(0),
      enterAt: above(0),
      label: word(words, 'messages'),
    },
    {
      from: 'client',
      to: 'consumer',
      exit: 'top',
      enter: 'bottom',
      exitAt: above(1),
      enterAt: above(1),
      label: word(words, 'pull'),
    },
    ...[0, 1, 2].map(
      (column): Link => ({
        from: 'client',
        to: ['ack', 'nak', 'term'][column]!,
        exitAt: above(column),
      }),
    ),
  ];
  return {boxes, links};
}
