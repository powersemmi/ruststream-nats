import {describe, expect, it} from 'vitest';
import {QUEUE_GROUP, SUBJECT, SUBSCRIPTIONS, matches, subjects} from './subjects';
import type {Words} from './words';

// Every word is its own key, so the test reads the layout and never a page's wording.
const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

describe('a subscription subject against a published subject', () => {
  it('matches an exact subject token for token', () => {
    expect(matches('orders.eu.created', 'orders.eu.created')).toBe(true);
    expect(matches('orders.eu.created', 'orders.us.created')).toBe(false);
  });

  it('takes * for exactly one token', () => {
    expect(matches('orders.*.created', 'orders.eu.created')).toBe(true);
    expect(matches('orders.*', 'orders.eu.created')).toBe(false);
    expect(matches('orders.*.created', 'orders.created')).toBe(false);
  });

  it('takes > for one or more trailing tokens, never none', () => {
    expect(matches('orders.>', 'orders.eu.created')).toBe(true);
    expect(matches('orders.>', 'orders.eu')).toBe(true);
    expect(matches('orders.>', 'orders')).toBe(false);
  });
});

describe('the subjects diagram', () => {
  const {boxes, links} = subjects(words);
  const reached = links.filter((link) => link.from === 'server').map((link) => link.to);

  it('delivers to every subscription whose subject matches, and to no other', () => {
    const matching = SUBSCRIPTIONS.filter((sub) => matches(sub.pattern, SUBJECT)).map(
      (sub) => sub.id,
    );
    expect(matching.length).toBeGreaterThan(0);
    expect(matching.length).toBeLessThan(SUBSCRIPTIONS.length);
    for (const sub of SUBSCRIPTIONS) {
      expect(reached.includes(sub.id), sub.id).toBe(matching.includes(sub.id));
    }
  });

  it('hands a matching queue group the message once, to one member', () => {
    expect(matches(QUEUE_GROUP.pattern, SUBJECT)).toBe(true);
    const members = QUEUE_GROUP.members.filter((member) => reached.includes(member));
    expect(members).toHaveLength(1);
  });

  it('writes each subject as the protocol spells it, whatever the language', () => {
    const lines = boxes.flatMap((box) => box.lines);
    for (const sub of SUBSCRIPTIONS) {
      expect(lines).toContain(sub.pattern);
    }
    expect(lines).toContain(SUBJECT);
  });
});
