import {describe, expect, it} from 'vitest';
import {resolveMarkdownLinks} from './markdownLinks';

// The hook as Docusaurus calls it: with the link and the page's path relative to the site.
const resolve = (url: string, sourceFilePath: string): string => {
  const hook = resolveMarkdownLinks() as (params: {url: string; sourceFilePath: string}) => string;
  return hook({url, sourceFilePath});
};

describe('a link Docusaurus cannot resolve', () => {
  it('from a translated page to an untranslated neighbour goes to its address', () => {
    expect(
      resolve(
        '../components/index.md',
        'i18n/ru/docusaurus-plugin-content-docs/current/architecture/index.md',
      ),
    ).toBe('/components/');
  });

  it('from an untranslated page to a translated one goes to its address, section included', () => {
    // A language build serves an English page in place of a missing translation, and finds its
    // neighbour by the translation's path, not by the English one the page names.
    expect(
      resolve('index.md#cost-of-the-code', '../docs/architecture/benchmarks/methodology.md'),
    ).toBe('/architecture/benchmarks/#cost-of-the-code');
  });

  it('to a file that does not exist fails the build', () => {
    expect(() => resolve('missing.md', '../docs/architecture/path.md')).toThrow(/does not resolve/);
  });

  it('to a file outside the documentation fails the build', () => {
    expect(() => resolve('../../README.md', '../docs/architecture/path.md')).toThrow(
      /does not resolve/,
    );
  });
});
