import {describe, expect, it} from 'vitest';
import {OUTCOMES, settlement} from './settlement';
import type {Words} from './words';

// Every word is its own key, so the test reads the layout and never a page's wording.
const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

describe('what a JetStream delivery sends for each outcome', () => {
  it('is the acknowledgement async-nats writes for the call the crate makes', () => {
    // `ack`, `AckKind::Nak(None)`, `AckKind::Nak(Some(delay))` and `AckKind::Term` in the
    // crate's delivery; an unsettled delivery sends nothing.
    expect(Object.fromEntries(OUTCOMES.map((outcome) => [outcome.id, outcome.wire]))).toEqual({
      ack: '+ACK',
      retry: '-NAK',
      retryAfter: '-NAK {"delay": …}',
      drop: '+TERM',
      unsettled: null,
    });
  });

  it('draws each outcome to its wire form and on to what the server does', () => {
    const {links} = settlement(words);
    for (const outcome of OUTCOMES) {
      const out = links.filter((link) => link.from === outcome.id).map((link) => link.to);
      expect(out).toEqual([`${outcome.id}Wire`]);
      const effect = links.filter((link) => link.from === `${outcome.id}Wire`).map((l) => l.to);
      expect(effect).toEqual([`${outcome.id}Effect`]);
    }
  });
});
