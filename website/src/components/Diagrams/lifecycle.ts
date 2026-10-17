import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * The broker's three states as types on the left, and on the right what each transition does
 * on NATS: the dial at connect, a SUB and a flush for each Core subscription, a stream lookup
 * and a consumer for each JetStream one, and the drain at shutdown.
 */
export function lifecycle(words: Words): {boxes: Box[]; links: Link[]} {
  const left = (id: string, y: number, href?: string): Box => ({
    id,
    x: 12,
    y,
    w: 156,
    h: 58,
    kind: 'broker',
    code: true,
    lines: lines(words, id),
    href,
  });
  const right = (id: string, y: number, {h = 58, href}: {h?: number; href?: string} = {}): Box => ({
    id,
    x: 192,
    y,
    w: 156,
    h,
    kind: 'core',
    lines: lines(words, id),
    href,
  });
  const boxes: Box[] = [
    {id: 'states', x: 0, y: 0, w: 176, h: 410, kind: 'group', lines: lines(words, 'states')},
    {id: 'nats', x: 184, y: 0, w: 176, h: 410, kind: 'group', lines: lines(words, 'nats')},
    left('unconnected', 28, '/components/broker/'),
    left('connected', 168),
    left('closed', 340, '/components/broker/shutdown/'),
    right('config', 28),
    right('dial', 98, {href: '/components/broker/connect-options/'}),
    right('core', 168, {href: '/components/subscriptions/core-subject/'}),
    right('jetstream', 252, {href: '/components/subscriptions/jetstream-subject/'}),
    right('drain', 340, {href: '/components/broker/shutdown/'}),
    {
      id: 'afterShutdown',
      x: 0,
      y: 432,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'afterShutdown'),
      href: '/components/broker/shutdown/',
    },
  ];
  const links: Link[] = [
    {from: 'unconnected', to: 'connected', label: word(words, 'connect')},
    {from: 'connected', to: 'closed', label: word(words, 'shutdown')},
    {from: 'config', to: 'dial'},
    {from: 'dial', to: 'core'},
    {from: 'core', to: 'jetstream'},
    {from: 'jetstream', to: 'drain'},
  ];
  return {boxes, links};
}
