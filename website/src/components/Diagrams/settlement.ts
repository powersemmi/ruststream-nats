import type {Box, Lane, Link} from './flow';
import {lines, word, type Words} from './words';

/**
 * What a JetStream delivery sends to its acknowledgement subject for each outcome of a handler,
 * as the protocol writes it, or `null` where nothing is sent.
 */
export const OUTCOMES = [
  {id: 'ack', wire: '+ACK'},
  {id: 'retry', wire: '-NAK'},
  {id: 'retryAfter', wire: '-NAK {"delay": …}'},
  {id: 'drop', wire: '+TERM'},
  {id: 'unsettled', wire: null},
] as const;

const COLUMN = 112;
const COLUMNS = [0, 124, 248] as const;
const ROW = 58;
const GAP = 12;
const TOP = 28;

const HREFS: Record<string, string> = {
  retryAfter: '/components/settlement/retry-after/',
  unsettledEffect: '/components/settlement/ack-wait/',
};

/*
 * The JetStream settlement table drawn as a timeline of three columns: what the handler returns,
 * what the delivery sends to the server, and what the server does with the message.
 */
export function settlement(words: Words): {boxes: Box[]; links: Link[]; lanes: Lane[]} {
  const cell = (id: string, column: number, row: number, fields: Partial<Box>): Box => ({
    id,
    x: COLUMNS[column]!,
    y: TOP + row * (ROW + GAP),
    w: COLUMN,
    h: ROW,
    kind: 'step',
    href: HREFS[id],
    ...fields,
    // A cell that brings its own lines, the protocol's, takes no words from the page.
    lines: fields.lines ?? lines(words, id),
  });
  const boxes: Box[] = OUTCOMES.flatMap((outcome, row) => [
    cell(outcome.id, 0, row, {kind: 'service', code: outcome.wire !== null}),
    cell(`${outcome.id}Wire`, 1, row, {
      kind: 'broker',
      code: outcome.wire !== null,
      // The delay of a negative acknowledgement goes on a line of its own, where it fits.
      lines: outcome.wire === null ? lines(words, 'nothing') : outcome.wire.split(/ (?=\{)/),
    }),
    cell(`${outcome.id}Effect`, 2, row, {
      kind: outcome.id === 'ack' ? 'good' : outcome.id === 'drop' ? 'bad' : 'core',
    }),
  ]);
  boxes.push({
    id: 'core',
    x: 0,
    y: TOP + OUTCOMES.length * (ROW + GAP) + 8,
    w: 360,
    h: 44,
    kind: 'note',
    lines: lines(words, 'core'),
    href: '/components/settlement/at-most-once/',
  });
  const links: Link[] = OUTCOMES.flatMap((outcome) => [
    {from: outcome.id, to: `${outcome.id}Wire`},
    {from: `${outcome.id}Wire`, to: `${outcome.id}Effect`},
  ]);
  const lanes: Lane[] = (['outcomeLane', 'wireLane', 'serverLane'] as const).map(
    (key, column) => ({x: COLUMNS[column]! + COLUMN / 2, label: word(words, key)}),
  );
  return {boxes, links, lanes};
}
