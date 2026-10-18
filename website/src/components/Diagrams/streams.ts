import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

// The messages the stream holds in the drawing, numbered by the stream from one.
export const STORED = 6;

const CELL = 40;
const STEP = 48;
const FRAME_Y = 96;

/*
 * A JetStream stream: a publish to one of its subjects is stored under the next sequence and
 * acknowledged back to the publisher, a repeated message id inside the window is stored once,
 * and each consumer reads the stream at a position of its own.
 */
export function streams(words: Words): {boxes: Box[]; links: Link[]} {
  const last = 12 + (STORED - 1) * STEP;
  const boxes: Box[] = [
    {
      id: 'publisher',
      x: 0,
      y: 0,
      w: 172,
      h: 58,
      kind: 'service',
      lines: lines(words, 'publisher'),
      href: '/components/publishing/jetstream-publish/',
    },
    {
      id: 'ack',
      x: 188,
      y: 0,
      w: 172,
      h: 58,
      kind: 'core',
      lines: lines(words, 'ack'),
      href: '/components/publishing/jetstream-publish/publish-ack/',
    },
    {id: 'stream', x: 0, y: FRAME_Y, w: 360, h: 116, kind: 'group', lines: lines(words, 'stream')},
  ];
  for (let sequence = 1; sequence <= STORED; sequence += 1) {
    boxes.push({
      id: `s${sequence}`,
      x: 12 + (sequence - 1) * STEP,
      y: FRAME_Y + 26,
      w: CELL,
      h: 30,
      kind: sequence === STORED ? 'good' : 'step',
      lines: [String(sequence)],
    });
  }
  boxes.push(
    {
      id: 'dedup',
      x: 12,
      y: FRAME_Y + 66,
      w: 336,
      h: 40,
      kind: 'note',
      lines: lines(words, 'dedup'),
      href: '/components/publishing/jetstream-publish/message-id/',
    },
    {
      id: 'durable',
      x: 0,
      y: 248,
      w: 200,
      h: 112,
      kind: 'group',
      lines: lines(words, 'durable'),
      href: '/components/subscriptions/durable/',
    },
    {id: 'replica1', x: 12, y: 274, w: 176, h: 34, kind: 'core', lines: lines(words, 'replica1')},
    {id: 'replica2', x: 12, y: 316, w: 176, h: 34, kind: 'core', lines: lines(words, 'replica2')},
    {
      id: 'ephemeral',
      x: 216,
      y: 248,
      w: 144,
      h: 112,
      kind: 'core',
      lines: lines(words, 'ephemeral'),
    },
    {id: 'positions', x: 0, y: 380, w: 360, h: 44, kind: 'note', lines: lines(words, 'positions')},
  );
  // The publish and its acknowledgement both meet the newest message, from the box above it.
  const publishX = 86;
  const ackX = last + CELL / 2;
  const links: Link[] = [
    {from: 'publisher', to: 'stream', enterAt: publishX / 360, label: word(words, 'publish')},
    {
      from: `s${STORED}`,
      to: 'ack',
      dashed: true,
      enterAt: (ackX - 188) / 172,
      label: word(words, 'acknowledged'),
    },
    {from: 'stream', to: 'durable', exitAt: 100 / 360, label: word(words, 'reads')},
    {from: 'stream', to: 'ephemeral', exitAt: 288 / 360, label: word(words, 'reads')},
  ];
  return {boxes, links};
}
