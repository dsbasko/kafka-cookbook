import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';

(globalThis as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;

const paramsRef: { current: Record<string, string | string[] | undefined> | null } = {
  current: { lang: 'ru' },
};
const pathnameRef: { current: string | null } = { current: '/ru/' };
const pushSpy = vi.fn();

vi.mock('next/navigation', () => ({
  useParams: () => paramsRef.current,
  usePathname: () => pathnameRef.current,
  useRouter: () => ({ push: pushSpy }),
}));

const setProseSize = vi.fn();
const setCodeSize = vi.fn();
const setProseFont = vi.fn();
const setCodeFont = vi.fn();

const ctxRef: {
  current: {
    prefs: { proseSize: 0 | 1 | 2 | 3; codeSize: 0 | 1 | 2 | 3; proseFont: 'serif' | 'sans' | 'slab'; codeFont: 'jetbrains' | 'fira' };
  };
} = {
  current: {
    prefs: { proseSize: 1, codeSize: 0, proseFont: 'serif', codeFont: 'jetbrains' },
  },
};

vi.mock('@/components/ReadingPrefsProvider', () => ({
  useReadingPrefs: () => ({
    prefs: ctxRef.current.prefs,
    setProseSize,
    setCodeSize,
    setProseFont,
    setCodeFont,
  }),
}));

const themePreferenceRef: { current: 'light' | 'dark' | 'system' } = { current: 'system' };
const setThemePreference = vi.fn();

vi.mock('@/components/ThemeProvider', () => ({
  useTheme: () => ({
    preference: themePreferenceRef.current,
    resolvedTheme: 'light',
    setPreference: setThemePreference,
  }),
}));

const writeStoredLangSpy = vi.fn();

vi.mock('@/lib/lang', async () => {
  const actual = await vi.importActual<typeof import('@/lib/lang')>('@/lib/lang');
  return {
    ...actual,
    writeStoredLang: writeStoredLangSpy,
  };
});

const { SettingsToggle } = await import('./SettingsToggle');

let container: HTMLDivElement;
let root: Root;

beforeEach(() => {
  paramsRef.current = { lang: 'ru' };
  pathnameRef.current = '/ru/';
  themePreferenceRef.current = 'system';
  setProseSize.mockReset();
  setCodeSize.mockReset();
  setProseFont.mockReset();
  setCodeFont.mockReset();
  setThemePreference.mockReset();
  writeStoredLangSpy.mockReset();
  pushSpy.mockReset();
  ctxRef.current = {
    prefs: { proseSize: 1, codeSize: 0, proseFont: 'serif', codeFont: 'jetbrains' },
  };
  container = document.createElement('div');
  document.body.appendChild(container);
  root = createRoot(container);
});

afterEach(() => {
  act(() => {
    root.unmount();
  });
  container.remove();
});

function renderToggle() {
  act(() => {
    root.render(<SettingsToggle />);
  });
}

function trigger(): HTMLButtonElement {
  const node = container.querySelector<HTMLButtonElement>('button[aria-haspopup="dialog"]');
  if (!node) throw new Error('toggle trigger not rendered');
  return node;
}

function popover(): HTMLDivElement {
  const node = container.querySelector<HTMLDivElement>('[role="dialog"]');
  if (!node) throw new Error('popover not rendered');
  return node;
}

function buttonByKind(kind: string): HTMLButtonElement {
  const node = container.querySelector<HTMLButtonElement>(`button[data-kind="${kind}"]`);
  if (!node) throw new Error(`button ${kind} not rendered`);
  return node;
}

function pillByProseFont(value: string): HTMLButtonElement {
  const node = container.querySelector<HTMLButtonElement>(`button[data-prose-font="${value}"]`);
  if (!node) throw new Error(`prose-font pill ${value} not rendered`);
  return node;
}

function pillByCodeFont(value: string): HTMLButtonElement {
  const node = container.querySelector<HTMLButtonElement>(`button[data-code-font="${value}"]`);
  if (!node) throw new Error(`code-font pill ${value} not rendered`);
  return node;
}

function pillByTheme(value: string): HTMLButtonElement {
  const node = container.querySelector<HTMLButtonElement>(`button[data-theme-pref="${value}"]`);
  if (!node) throw new Error(`theme pill ${value} not rendered`);
  return node;
}

function pillByLang(value: string): HTMLButtonElement {
  const node = container.querySelector<HTMLButtonElement>(`button[data-lang-pref="${value}"]`);
  if (!node) throw new Error(`lang pill ${value} not rendered`);
  return node;
}

function click(node: HTMLElement) {
  act(() => {
    node.dispatchEvent(new MouseEvent('mousedown', { bubbles: true }));
    node.dispatchEvent(new MouseEvent('click', { bubbles: true }));
  });
}

function pressKey(key: string) {
  act(() => {
    document.dispatchEvent(new KeyboardEvent('keydown', { key, bubbles: true }));
  });
}

describe('SettingsToggle', () => {
  it('renders the trigger with localized aria-label and a closed popover', () => {
    renderToggle();
    expect(trigger().getAttribute('aria-expanded')).toBe('false');
    expect(trigger().getAttribute('aria-label')).toBe('Настройки');
    expect(popover().hasAttribute('hidden')).toBe(true);
  });

  it('opens the popover on trigger click and closes on second click', () => {
    renderToggle();
    click(trigger());
    expect(trigger().getAttribute('aria-expanded')).toBe('true');
    expect(popover().hasAttribute('hidden')).toBe(false);
    click(trigger());
    expect(trigger().getAttribute('aria-expanded')).toBe('false');
    expect(popover().hasAttribute('hidden')).toBe(true);
  });

  it('closes the popover when Escape is pressed', () => {
    renderToggle();
    click(trigger());
    expect(popover().hasAttribute('hidden')).toBe(false);
    pressKey('Escape');
    expect(popover().hasAttribute('hidden')).toBe(true);
  });

  it('closes the popover on outside mousedown', () => {
    renderToggle();
    click(trigger());
    expect(popover().hasAttribute('hidden')).toBe(false);
    const outside = document.createElement('button');
    document.body.appendChild(outside);
    act(() => {
      outside.dispatchEvent(new MouseEvent('mousedown', { bubbles: true }));
    });
    expect(popover().hasAttribute('hidden')).toBe(true);
    outside.remove();
  });

  it('marks the active theme pill via aria-checked and data-active', () => {
    themePreferenceRef.current = 'dark';
    renderToggle();
    click(trigger());
    expect(pillByTheme('dark').getAttribute('aria-checked')).toBe('true');
    expect(pillByTheme('dark').getAttribute('data-active')).toBe('true');
    expect(pillByTheme('light').getAttribute('aria-checked')).toBe('false');
  });

  it('clicking a theme pill calls setPreference but keeps the popover open', () => {
    renderToggle();
    click(trigger());
    click(pillByTheme('dark'));
    expect(setThemePreference).toHaveBeenCalledWith('dark');
    expect(popover().hasAttribute('hidden')).toBe(false);
  });

  it('marks the active language pill based on the current pathname', () => {
    pathnameRef.current = '/en/some-lesson';
    renderToggle();
    click(trigger());
    expect(pillByLang('en').getAttribute('aria-checked')).toBe('true');
    expect(pillByLang('ru').getAttribute('aria-checked')).toBe('false');
  });

  it('clicking a non-current language pill writes the stored lang and navigates', () => {
    pathnameRef.current = '/ru/module-1/lesson-2';
    renderToggle();
    click(trigger());
    click(pillByLang('en'));
    expect(writeStoredLangSpy).toHaveBeenCalledWith('en');
    expect(pushSpy).toHaveBeenCalledWith('/en/module-1/lesson-2');
  });

  it('clicking the current language pill does not navigate', () => {
    pathnameRef.current = '/ru/';
    renderToggle();
    click(trigger());
    click(pillByLang('ru'));
    expect(writeStoredLangSpy).toHaveBeenCalledWith('ru');
    expect(pushSpy).not.toHaveBeenCalled();
  });

  it('renders the current prose size label in px', () => {
    ctxRef.current.prefs = { proseSize: 2, codeSize: 0, proseFont: 'serif', codeFont: 'jetbrains' };
    renderToggle();
    click(trigger());
    expect(container.querySelector('[data-kind="prose-value"]')?.textContent).toBe('18px');
  });

  it('renders the current code size label in px', () => {
    ctxRef.current.prefs = { proseSize: 1, codeSize: 3, proseFont: 'serif', codeFont: 'jetbrains' };
    renderToggle();
    click(trigger());
    expect(container.querySelector('[data-kind="code-value"]')?.textContent).toBe('20px');
  });

  it('disables A− for prose when proseSize === 0', () => {
    ctxRef.current.prefs = { proseSize: 0, codeSize: 0, proseFont: 'serif', codeFont: 'jetbrains' };
    renderToggle();
    click(trigger());
    expect(buttonByKind('prose-decrease').disabled).toBe(true);
    expect(buttonByKind('prose-increase').disabled).toBe(false);
  });

  it('disables A+ for prose when proseSize === 3', () => {
    ctxRef.current.prefs = { proseSize: 3, codeSize: 0, proseFont: 'serif', codeFont: 'jetbrains' };
    renderToggle();
    click(trigger());
    expect(buttonByKind('prose-decrease').disabled).toBe(false);
    expect(buttonByKind('prose-increase').disabled).toBe(true);
  });

  it('disables A− for code when codeSize === 0', () => {
    ctxRef.current.prefs = { proseSize: 1, codeSize: 0, proseFont: 'serif', codeFont: 'jetbrains' };
    renderToggle();
    click(trigger());
    expect(buttonByKind('code-decrease').disabled).toBe(true);
    expect(buttonByKind('code-increase').disabled).toBe(false);
  });

  it('disables A+ for code when codeSize === 3', () => {
    ctxRef.current.prefs = { proseSize: 1, codeSize: 3, proseFont: 'serif', codeFont: 'jetbrains' };
    renderToggle();
    click(trigger());
    expect(buttonByKind('code-decrease').disabled).toBe(false);
    expect(buttonByKind('code-increase').disabled).toBe(true);
  });

  it('A+ for prose calls setProseSize with the next step', () => {
    ctxRef.current.prefs = { proseSize: 1, codeSize: 0, proseFont: 'serif', codeFont: 'jetbrains' };
    renderToggle();
    click(trigger());
    click(buttonByKind('prose-increase'));
    expect(setProseSize).toHaveBeenCalledTimes(1);
    expect(setProseSize).toHaveBeenCalledWith(2);
  });

  it('A− for prose calls setProseSize with the previous step', () => {
    ctxRef.current.prefs = { proseSize: 2, codeSize: 0, proseFont: 'serif', codeFont: 'jetbrains' };
    renderToggle();
    click(trigger());
    click(buttonByKind('prose-decrease'));
    expect(setProseSize).toHaveBeenCalledWith(1);
  });

  it('A+ for code calls setCodeSize with the next step', () => {
    ctxRef.current.prefs = { proseSize: 1, codeSize: 1, proseFont: 'serif', codeFont: 'jetbrains' };
    renderToggle();
    click(trigger());
    click(buttonByKind('code-increase'));
    expect(setCodeSize).toHaveBeenCalledWith(2);
  });

  it('A− for code calls setCodeSize with the previous step', () => {
    ctxRef.current.prefs = { proseSize: 1, codeSize: 2, proseFont: 'serif', codeFont: 'jetbrains' };
    renderToggle();
    click(trigger());
    click(buttonByKind('code-decrease'));
    expect(setCodeSize).toHaveBeenCalledWith(1);
  });

  it('clicking the Inter pill calls setProseFont("sans")', () => {
    renderToggle();
    click(trigger());
    click(pillByProseFont('sans'));
    expect(setProseFont).toHaveBeenCalledTimes(1);
    expect(setProseFont).toHaveBeenCalledWith('sans');
  });

  it('clicking the Roboto Slab pill calls setProseFont("slab")', () => {
    renderToggle();
    click(trigger());
    click(pillByProseFont('slab'));
    expect(setProseFont).toHaveBeenCalledWith('slab');
  });

  it('clicking the Fira Code pill calls setCodeFont("fira")', () => {
    renderToggle();
    click(trigger());
    click(pillByCodeFont('fira'));
    expect(setCodeFont).toHaveBeenCalledWith('fira');
  });

  it('marks the active prose-font pill via aria-checked and data-active', () => {
    ctxRef.current.prefs = { proseSize: 1, codeSize: 0, proseFont: 'slab', codeFont: 'jetbrains' };
    renderToggle();
    click(trigger());
    expect(pillByProseFont('slab').getAttribute('aria-checked')).toBe('true');
    expect(pillByProseFont('slab').getAttribute('data-active')).toBe('true');
    expect(pillByProseFont('serif').getAttribute('aria-checked')).toBe('false');
  });

  it('marks the active code-font pill via aria-checked and data-active', () => {
    ctxRef.current.prefs = { proseSize: 1, codeSize: 0, proseFont: 'serif', codeFont: 'fira' };
    renderToggle();
    click(trigger());
    expect(pillByCodeFont('fira').getAttribute('aria-checked')).toBe('true');
    expect(pillByCodeFont('jetbrains').getAttribute('aria-checked')).toBe('false');
  });

  it('falls into english labels when lang param is en', () => {
    paramsRef.current = { lang: 'en' };
    pathnameRef.current = '/en/';
    renderToggle();
    expect(trigger().getAttribute('aria-label')).toBe('Settings');
  });
});
