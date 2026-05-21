import { afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';

(globalThis as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;

const { ReadingPrefsProvider, useReadingPrefs } = await import('./ReadingPrefsProvider');
const { READING_PREFS_STORAGE_KEY, DEFAULT_PREFS } = await import('@/lib/reading-prefs');

type Captured = ReturnType<typeof useReadingPrefs> | null;

function Capture({ into }: { into: { current: Captured } }) {
  into.current = useReadingPrefs();
  return null;
}

let container: HTMLDivElement;
let root: Root;

beforeAll(() => {
  if (typeof window.localStorage?.setItem !== 'function') {
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
  }
});

beforeEach(() => {
  window.localStorage.clear();
  document.documentElement.removeAttribute('data-prose-size');
  document.documentElement.removeAttribute('data-code-size');
  document.documentElement.removeAttribute('data-prose-font');
  document.documentElement.removeAttribute('data-code-font');
  container = document.createElement('div');
  document.body.appendChild(container);
  root = createRoot(container);
});

afterEach(() => {
  act(() => {
    root.unmount();
  });
  container.remove();
  document.documentElement.removeAttribute('data-prose-size');
  document.documentElement.removeAttribute('data-code-size');
  document.documentElement.removeAttribute('data-prose-font');
  document.documentElement.removeAttribute('data-code-font');
});

function render(captured: { current: Captured }) {
  act(() => {
    root.render(
      <ReadingPrefsProvider>
        <Capture into={captured} />
      </ReadingPrefsProvider>,
    );
  });
}

describe('ReadingPrefsProvider', () => {
  it('reads prefs from <html> data-* attributes on mount', () => {
    document.documentElement.setAttribute('data-prose-size', '3');
    document.documentElement.setAttribute('data-code-size', '1');
    document.documentElement.setAttribute('data-prose-font', 'slab');
    document.documentElement.setAttribute('data-code-font', 'fira');
    const captured: { current: Captured } = { current: null };
    render(captured);
    expect(captured.current?.prefs).toEqual({
      proseSize: 3,
      codeSize: 1,
      proseFont: 'slab',
      codeFont: 'fira',
    });
  });

  it('falls back to defaults when <html> has no data-* attributes', () => {
    const captured: { current: Captured } = { current: null };
    render(captured);
    expect(captured.current?.prefs).toEqual(DEFAULT_PREFS);
  });

  it('setProseSize updates state, <html>, and localStorage', () => {
    const captured: { current: Captured } = { current: null };
    render(captured);
    act(() => {
      captured.current?.setProseSize(3);
    });
    expect(captured.current?.prefs.proseSize).toBe(3);
    expect(document.documentElement.dataset.proseSize).toBe('3');
    const raw = window.localStorage.getItem(READING_PREFS_STORAGE_KEY);
    expect(raw).not.toBeNull();
    expect(JSON.parse(raw!).proseSize).toBe(3);
  });

  it('setCodeSize updates state, <html>, and localStorage', () => {
    const captured: { current: Captured } = { current: null };
    render(captured);
    act(() => {
      captured.current?.setCodeSize(3);
    });
    expect(captured.current?.prefs.codeSize).toBe(3);
    expect(document.documentElement.dataset.codeSize).toBe('3');
    expect(JSON.parse(window.localStorage.getItem(READING_PREFS_STORAGE_KEY)!).codeSize).toBe(3);
  });

  it('setProseFont updates state, <html>, and localStorage', () => {
    const captured: { current: Captured } = { current: null };
    render(captured);
    act(() => {
      captured.current?.setProseFont('sans');
    });
    expect(captured.current?.prefs.proseFont).toBe('sans');
    expect(document.documentElement.dataset.proseFont).toBe('sans');
    expect(JSON.parse(window.localStorage.getItem(READING_PREFS_STORAGE_KEY)!).proseFont).toBe('sans');
  });

  it('setCodeFont updates state, <html>, and localStorage', () => {
    const captured: { current: Captured } = { current: null };
    render(captured);
    act(() => {
      captured.current?.setCodeFont('fira');
    });
    expect(captured.current?.prefs.codeFont).toBe('fira');
    expect(document.documentElement.dataset.codeFont).toBe('fira');
    expect(JSON.parse(window.localStorage.getItem(READING_PREFS_STORAGE_KEY)!).codeFont).toBe('fira');
  });

  it('persists the full prefs object on every setter call', () => {
    const captured: { current: Captured } = { current: null };
    render(captured);
    act(() => {
      captured.current?.setProseSize(3);
    });
    act(() => {
      captured.current?.setCodeFont('fira');
    });
    const stored = JSON.parse(window.localStorage.getItem(READING_PREFS_STORAGE_KEY)!);
    expect(stored).toEqual({
      proseSize: 3,
      codeSize: DEFAULT_PREFS.codeSize,
      proseFont: DEFAULT_PREFS.proseFont,
      codeFont: 'fira',
    });
  });

  it('ignores storage events for unrelated keys', () => {
    const captured: { current: Captured } = { current: null };
    render(captured);
    const before = captured.current?.prefs;
    act(() => {
      window.dispatchEvent(
        new StorageEvent('storage', { key: 'some-other-key', newValue: 'whatever' }),
      );
    });
    expect(captured.current?.prefs).toEqual(before);
  });

  it('syncs state from localStorage when a storage event for the prefs key fires', () => {
    const captured: { current: Captured } = { current: null };
    render(captured);
    const incoming = { proseSize: 3, codeSize: 0, proseFont: 'slab', codeFont: 'fira' };
    window.localStorage.setItem(READING_PREFS_STORAGE_KEY, JSON.stringify(incoming));
    act(() => {
      window.dispatchEvent(
        new StorageEvent('storage', {
          key: READING_PREFS_STORAGE_KEY,
          newValue: JSON.stringify(incoming),
        }),
      );
    });
    expect(captured.current?.prefs).toEqual(incoming);
    expect(document.documentElement.dataset.proseSize).toBe('3');
    expect(document.documentElement.dataset.codeSize).toBe('0');
    expect(document.documentElement.dataset.proseFont).toBe('slab');
    expect(document.documentElement.dataset.codeFont).toBe('fira');
  });

});

describe('useReadingPrefs', () => {
  it('throws when used outside the provider', () => {
    const errors: unknown[] = [];
    function Probe() {
      try {
        useReadingPrefs();
      } catch (err) {
        errors.push(err);
      }
      return null;
    }
    const originalError = console.error;
    console.error = () => {};
    try {
      act(() => {
        root.render(<Probe />);
      });
    } finally {
      console.error = originalError;
    }
    expect(errors).toHaveLength(1);
    expect((errors[0] as Error).message).toMatch(/ReadingPrefsProvider/);
  });
});
