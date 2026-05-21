import { afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import {
  applyPrefs,
  DEFAULT_PREFS,
  isCodeFont,
  isProseFont,
  isSizeStep,
  READING_PREFS_INIT_SCRIPT,
  READING_PREFS_STORAGE_KEY,
  readStoredPrefs,
  writeStoredPrefs,
} from './reading-prefs';

beforeAll(() => {
  if (typeof window.localStorage?.setItem === 'function') return;
  const store = new Map<string, string>();
  const shim: Storage = {
    get length() {
      return store.size;
    },
    clear: () => store.clear(),
    getItem: (key: string) => (store.has(key) ? store.get(key)! : null),
    setItem: (key: string, value: string) => void store.set(key, String(value)),
    removeItem: (key: string) => void store.delete(key),
    key: (index: number) => Array.from(store.keys())[index] ?? null,
  };
  Object.defineProperty(window, 'localStorage', {
    configurable: true,
    value: shim,
  });
});

function clearHtmlAttrs() {
  const el = document.documentElement;
  el.removeAttribute('data-prose-size');
  el.removeAttribute('data-code-size');
  el.removeAttribute('data-prose-font');
  el.removeAttribute('data-code-font');
}

describe('isProseFont', () => {
  it('accepts serif, sans, slab', () => {
    expect(isProseFont('serif')).toBe(true);
    expect(isProseFont('sans')).toBe(true);
    expect(isProseFont('slab')).toBe(true);
  });

  it('rejects everything else', () => {
    expect(isProseFont('SERIF')).toBe(false);
    expect(isProseFont('lora')).toBe(false);
    expect(isProseFont('')).toBe(false);
    expect(isProseFont(null)).toBe(false);
    expect(isProseFont(undefined)).toBe(false);
    expect(isProseFont(42)).toBe(false);
  });
});

describe('isCodeFont', () => {
  it('accepts jetbrains, fira', () => {
    expect(isCodeFont('jetbrains')).toBe(true);
    expect(isCodeFont('fira')).toBe(true);
  });

  it('rejects everything else', () => {
    expect(isCodeFont('plex')).toBe(false);
    expect(isCodeFont('mono')).toBe(false);
    expect(isCodeFont(null)).toBe(false);
    expect(isCodeFont(3)).toBe(false);
  });
});

describe('isSizeStep', () => {
  it('accepts 0..5', () => {
    expect(isSizeStep(0)).toBe(true);
    expect(isSizeStep(1)).toBe(true);
    expect(isSizeStep(2)).toBe(true);
    expect(isSizeStep(3)).toBe(true);
    expect(isSizeStep(4)).toBe(true);
    expect(isSizeStep(5)).toBe(true);
  });

  it('rejects out of range and non-integers', () => {
    expect(isSizeStep(-1)).toBe(false);
    expect(isSizeStep(6)).toBe(false);
    expect(isSizeStep(7)).toBe(false);
    expect(isSizeStep(1.5)).toBe(false);
    expect(isSizeStep('2')).toBe(false);
    expect(isSizeStep(null)).toBe(false);
  });
});

describe('readStoredPrefs / writeStoredPrefs', () => {
  beforeEach(() => {
    window.localStorage.clear();
  });

  it('returns defaults when nothing is stored', () => {
    expect(readStoredPrefs()).toEqual(DEFAULT_PREFS);
  });

  it('returns defaults when JSON is malformed', () => {
    window.localStorage.setItem(READING_PREFS_STORAGE_KEY, 'not-json{');
    expect(readStoredPrefs()).toEqual(DEFAULT_PREFS);
  });

  it('returns defaults when stored value is not an object', () => {
    window.localStorage.setItem(READING_PREFS_STORAGE_KEY, '"oops"');
    expect(readStoredPrefs()).toEqual(DEFAULT_PREFS);
    window.localStorage.setItem(READING_PREFS_STORAGE_KEY, 'null');
    expect(readStoredPrefs()).toEqual(DEFAULT_PREFS);
  });

  it('keeps valid fields and substitutes defaults for invalid ones', () => {
    window.localStorage.setItem(
      READING_PREFS_STORAGE_KEY,
      JSON.stringify({
        proseSize: 3,
        codeSize: 99,
        proseFont: 'slab',
        codeFont: 'unknown',
      }),
    );
    expect(readStoredPrefs()).toEqual({
      proseSize: 3,
      codeSize: DEFAULT_PREFS.codeSize,
      proseFont: 'slab',
      codeFont: DEFAULT_PREFS.codeFont,
    });
  });

  it('round-trips a full prefs object', () => {
    const prefs = { proseSize: 3, codeSize: 1, proseFont: 'sans', codeFont: 'fira' } as const;
    writeStoredPrefs(prefs);
    expect(window.localStorage.getItem(READING_PREFS_STORAGE_KEY)).toBe(JSON.stringify(prefs));
    expect(readStoredPrefs()).toEqual(prefs);
  });
});

describe('applyPrefs', () => {
  afterEach(() => {
    clearHtmlAttrs();
  });

  it('writes all four data-* attributes', () => {
    applyPrefs({ proseSize: 3, codeSize: 2, proseFont: 'sans', codeFont: 'fira' });
    expect(document.documentElement.dataset.proseSize).toBe('3');
    expect(document.documentElement.dataset.codeSize).toBe('2');
    expect(document.documentElement.dataset.proseFont).toBe('sans');
    expect(document.documentElement.dataset.codeFont).toBe('fira');
  });
});

describe('READING_PREFS_INIT_SCRIPT', () => {
  beforeEach(() => {
    window.localStorage.clear();
    clearHtmlAttrs();
  });

  afterEach(() => {
    clearHtmlAttrs();
  });

  it('writes defaults when nothing is stored', () => {
    new Function(READING_PREFS_INIT_SCRIPT)();
    expect(document.documentElement.dataset.proseSize).toBe(String(DEFAULT_PREFS.proseSize));
    expect(document.documentElement.dataset.codeSize).toBe(String(DEFAULT_PREFS.codeSize));
    expect(document.documentElement.dataset.proseFont).toBe(DEFAULT_PREFS.proseFont);
    expect(document.documentElement.dataset.codeFont).toBe(DEFAULT_PREFS.codeFont);
  });

  it('uses stored values when fully valid', () => {
    window.localStorage.setItem(
      READING_PREFS_STORAGE_KEY,
      JSON.stringify({ proseSize: 3, codeSize: 1, proseFont: 'slab', codeFont: 'fira' }),
    );
    new Function(READING_PREFS_INIT_SCRIPT)();
    expect(document.documentElement.dataset.proseSize).toBe('3');
    expect(document.documentElement.dataset.codeSize).toBe('1');
    expect(document.documentElement.dataset.proseFont).toBe('slab');
    expect(document.documentElement.dataset.codeFont).toBe('fira');
  });

  it('falls back to defaults when JSON is malformed', () => {
    window.localStorage.setItem(READING_PREFS_STORAGE_KEY, 'broken{json');
    new Function(READING_PREFS_INIT_SCRIPT)();
    expect(document.documentElement.dataset.proseSize).toBe(String(DEFAULT_PREFS.proseSize));
    expect(document.documentElement.dataset.codeSize).toBe(String(DEFAULT_PREFS.codeSize));
    expect(document.documentElement.dataset.proseFont).toBe(DEFAULT_PREFS.proseFont);
    expect(document.documentElement.dataset.codeFont).toBe(DEFAULT_PREFS.codeFont);
  });

  it('partially valid object keeps good fields, defaults the rest', () => {
    window.localStorage.setItem(
      READING_PREFS_STORAGE_KEY,
      JSON.stringify({ proseSize: 3, codeSize: 'huge', proseFont: 'sans', codeFont: 9 }),
    );
    new Function(READING_PREFS_INIT_SCRIPT)();
    expect(document.documentElement.dataset.proseSize).toBe('3');
    expect(document.documentElement.dataset.codeSize).toBe(String(DEFAULT_PREFS.codeSize));
    expect(document.documentElement.dataset.proseFont).toBe('sans');
    expect(document.documentElement.dataset.codeFont).toBe(DEFAULT_PREFS.codeFont);
  });
});
