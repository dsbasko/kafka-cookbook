import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  applyPreference,
  applyResolvedTheme,
  getSystemTheme,
  isThemePreference,
  readStoredPreference,
  resolveTheme,
  THEME_INIT_SCRIPT,
  THEME_STORAGE_KEY,
  writeStoredPreference,
} from './theme';

function mockMatchMedia(matches: boolean) {
  Object.defineProperty(window, 'matchMedia', {
    configurable: true,
    writable: true,
    value: (query: string) => ({
      matches,
      media: query,
      onchange: null,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      addListener: vi.fn(),
      removeListener: vi.fn(),
      dispatchEvent: vi.fn(),
    }),
  });
}

// jsdom in this project ships without a working Storage implementation,
// so install a minimal in-memory shim shared across the test file.
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

describe('isThemePreference', () => {
  it('accepts light, dark, paper', () => {
    expect(isThemePreference('light')).toBe(true);
    expect(isThemePreference('dark')).toBe(true);
    expect(isThemePreference('paper')).toBe(true);
  });

  it('rejects legacy "system" and everything else', () => {
    expect(isThemePreference('system')).toBe(false);
    expect(isThemePreference('')).toBe(false);
    expect(isThemePreference(null)).toBe(false);
    expect(isThemePreference(undefined)).toBe(false);
    expect(isThemePreference('Light')).toBe(false);
    expect(isThemePreference('sepia')).toBe(false);
    expect(isThemePreference(42)).toBe(false);
  });
});

describe('getSystemTheme', () => {
  afterEach(() => {
    Reflect.deleteProperty(window, 'matchMedia');
  });

  it('returns dark when prefers-color-scheme: dark matches', () => {
    mockMatchMedia(true);
    expect(getSystemTheme()).toBe('dark');
  });

  it('returns light when prefers-color-scheme: dark does not match', () => {
    mockMatchMedia(false);
    expect(getSystemTheme()).toBe('light');
  });
});

describe('resolveTheme', () => {
  it('returns the preference unchanged (no system intermediate)', () => {
    expect(resolveTheme('light')).toBe('light');
    expect(resolveTheme('dark')).toBe('dark');
    expect(resolveTheme('paper')).toBe('paper');
  });
});

describe('readStoredPreference / writeStoredPreference', () => {
  beforeEach(() => {
    window.localStorage.clear();
  });

  afterEach(() => {
    Reflect.deleteProperty(window, 'matchMedia');
  });

  it('falls back to system query when nothing is stored', () => {
    mockMatchMedia(true);
    expect(readStoredPreference()).toBe('dark');
    mockMatchMedia(false);
    expect(readStoredPreference()).toBe('light');
  });

  it('falls back to system query when stored value is invalid or legacy "system"', () => {
    mockMatchMedia(false);
    window.localStorage.setItem(THEME_STORAGE_KEY, 'turquoise');
    expect(readStoredPreference()).toBe('light');
    window.localStorage.setItem(THEME_STORAGE_KEY, 'system');
    expect(readStoredPreference()).toBe('light');
  });

  it('round-trips a valid preference', () => {
    writeStoredPreference('dark');
    expect(window.localStorage.getItem(THEME_STORAGE_KEY)).toBe('dark');
    expect(readStoredPreference()).toBe('dark');
    writeStoredPreference('paper');
    expect(readStoredPreference()).toBe('paper');
  });
});

describe('applyResolvedTheme / applyPreference', () => {
  afterEach(() => {
    document.documentElement.removeAttribute('data-theme');
  });

  it('writes the resolved theme to documentElement.dataset', () => {
    applyResolvedTheme('dark');
    expect(document.documentElement.dataset.theme).toBe('dark');
    applyResolvedTheme('light');
    expect(document.documentElement.dataset.theme).toBe('light');
    applyResolvedTheme('paper');
    expect(document.documentElement.dataset.theme).toBe('paper');
  });

  it('applies a preference and returns it unchanged', () => {
    expect(applyPreference('paper')).toBe('paper');
    expect(document.documentElement.dataset.theme).toBe('paper');
    expect(applyPreference('light')).toBe('light');
    expect(document.documentElement.dataset.theme).toBe('light');
  });
});

describe('THEME_INIT_SCRIPT', () => {
  beforeEach(() => {
    window.localStorage.clear();
    document.documentElement.removeAttribute('data-theme');
  });

  afterEach(() => {
    Reflect.deleteProperty(window, 'matchMedia');
    document.documentElement.removeAttribute('data-theme');
  });

  it('uses stored preference when present', () => {
    window.localStorage.setItem(THEME_STORAGE_KEY, 'dark');
    mockMatchMedia(false);
    new Function(THEME_INIT_SCRIPT)();
    expect(document.documentElement.dataset.theme).toBe('dark');
  });

  it('respects a stored "paper" preference without falling back to system', () => {
    window.localStorage.setItem(THEME_STORAGE_KEY, 'paper');
    mockMatchMedia(true);
    new Function(THEME_INIT_SCRIPT)();
    expect(document.documentElement.dataset.theme).toBe('paper');
  });

  it('silently migrates legacy "system" via matchMedia', () => {
    window.localStorage.setItem(THEME_STORAGE_KEY, 'system');
    mockMatchMedia(true);
    new Function(THEME_INIT_SCRIPT)();
    expect(document.documentElement.dataset.theme).toBe('dark');
  });

  it('defaults to system query when nothing is stored', () => {
    mockMatchMedia(false);
    new Function(THEME_INIT_SCRIPT)();
    expect(document.documentElement.dataset.theme).toBe('light');
  });
});
