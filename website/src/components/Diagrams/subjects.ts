import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

// The subject the drawing publishes, and the subscriptions the server holds when it arrives.
// Subjects are protocol text, written as the wire spells them in every language.
export const SUBJECT = 'orders.eu.created';

export const SUBSCRIPTIONS = [
  {id: 'exact', pattern: 'orders.eu.created'},
  {id: 'one', pattern: 'orders.*.created'},
  {id: 'rest', pattern: 'orders.>'},
  {id: 'other', pattern: 'orders.us.*'},
] as const;

export const QUEUE_GROUP = {pattern: 'orders.*.created', members: ['member1', 'member2']} as const;

/**
 * Whether a subscription subject takes a published subject, token by token: `*` stands for one
 * token, and `>`, last, for one or more.
 */
export function matches(pattern: string, subject: string): boolean {
  const want = pattern.split('.');
  const have = subject.split('.');
  for (const [index, token] of want.entries()) {
    if (token === '>') {
      return index === want.length - 1 && have.length > index;
    }
    if (index >= have.length || (token !== '*' && token !== have[index])) {
      return false;
    }
  }
  return want.length === have.length;
}

const SERVER_W = 120;
const SUB_X = 150;
const SUB_W = 210;
const SUB_H = 44;
const SERVER_TOP = 90;
const MEMBER_H = 44;
// The queue group's frame: its name, then its members one under another.
const GROUP_H = 26 + QUEUE_GROUP.members.length * (MEMBER_H + 8);
// The server reaches down past the last member, so every match leaves it straight across.
const SERVER_H = SUBSCRIPTIONS.length * (SUB_H + 12) + GROUP_H;

/*
 * Core NATS routing: one publish, the server matching its subject against every subscription it
 * holds at that moment, a copy to each match, one member of a queue group, and nothing kept.
 */
export function subjects(words: Words): {boxes: Box[]; links: Link[]} {
  const boxes: Box[] = [
    {
      id: 'publisher',
      x: 0,
      y: 0,
      w: SERVER_W,
      h: 58,
      kind: 'service',
      lines: [...lines(words, 'publisher'), SUBJECT],
    },
    {
      id: 'server',
      x: 0,
      y: SERVER_TOP,
      w: SERVER_W,
      h: SERVER_H,
      kind: 'broker',
      lines: lines(words, 'server'),
    },
  ];
  const levels: Record<string, number> = {};
  SUBSCRIPTIONS.forEach((sub, index) => {
    const y = SERVER_TOP + index * (SUB_H + 12);
    levels[sub.id] = y + SUB_H / 2;
    boxes.push({
      id: sub.id,
      x: SUB_X,
      y,
      w: SUB_W,
      h: SUB_H,
      kind: matches(sub.pattern, SUBJECT) ? 'good' : 'step',
      lines: [sub.pattern, ...lines(words, sub.id)],
    });
  });
  const groupY = SERVER_TOP + SUBSCRIPTIONS.length * (SUB_H + 12);
  boxes.push({
    id: 'queue',
    x: SUB_X - 10,
    y: groupY,
    w: SUB_W + 10,
    h: GROUP_H,
    kind: 'group',
    lines: lines(words, 'queue'),
    href: '/components/subscriptions/queue-group/',
  });
  QUEUE_GROUP.members.forEach((member, index) => {
    const y = groupY + 26 + index * (MEMBER_H + 8);
    levels[member] = y + MEMBER_H / 2;
    boxes.push({
      id: member,
      x: SUB_X,
      y,
      w: SUB_W - 10,
      h: MEMBER_H,
      // The server picks one member of the group for each message.
      kind: index === 0 ? 'good' : 'step',
      lines: [QUEUE_GROUP.pattern, ...lines(words, member)],
    });
  });
  boxes.push({
    id: 'dropped',
    x: 0,
    y: SERVER_TOP + SERVER_H + 20,
    w: 360,
    h: 44,
    kind: 'note',
    lines: lines(words, 'dropped'),
  });
  const along = (id: string): number => (levels[id]! - SERVER_TOP) / SERVER_H;
  const reached = [
    ...SUBSCRIPTIONS.filter((sub) => matches(sub.pattern, SUBJECT)).map((sub) => sub.id),
    ...(matches(QUEUE_GROUP.pattern, SUBJECT) ? [QUEUE_GROUP.members[0]] : []),
  ];
  const links: Link[] = [
    {from: 'publisher', to: 'server', label: word(words, 'publishes')},
    ...reached.map(
      (id): Link => ({from: 'server', to: id, exit: 'right', enter: 'left', exitAt: along(id)}),
    ),
  ];
  return {boxes, links};
}
