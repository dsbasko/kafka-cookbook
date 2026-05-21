export type ThemePreference = 'light' | 'dark' | 'paper';
export type ResolvedTheme = ThemePreference;

export const THEME_STORAGE_KEY = 'kafka-cookbook-theme';
export const THEME_PREFERENCES = ['light', 'paper', 'dark'] as const;

export function isThemePreference(value: unknown): value is ThemePreference {
  return value === 'light' || value === 'dark' || value === 'paper';
}

export function getSystemTheme(): 'light' | 'dark' {
  if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') {
    return 'light';
  }
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

export function resolveTheme(preference: ThemePreference): ResolvedTheme {
  return preference;
}

/**
 * Returns the stored preference or — when nothing valid is stored —
 * silently picks light/dark from `prefers-color-scheme`. This preserves the
 * old "system" auto-detect for first-time visitors and for users migrating
 * from previously-stored `'system'` without exposing it as a UI choice.
 */
export function readStoredPreference(): ThemePreference {
  if (typeof window === 'undefined') return 'light';
  try {
    const raw = window.localStorage.getItem(THEME_STORAGE_KEY);
    if (isThemePreference(raw)) return raw;
    return getSystemTheme();
  } catch {
    return 'light';
  }
}

export function writeStoredPreference(preference: ThemePreference): void {
  if (typeof window === 'undefined') return;
  try {
    window.localStorage.setItem(THEME_STORAGE_KEY, preference);
  } catch {
    /* storage may be unavailable (private mode, quota); ignore. */
  }
}

export function applyResolvedTheme(theme: ResolvedTheme): void {
  if (typeof document === 'undefined') return;
  document.documentElement.dataset.theme = theme;
}

export function applyPreference(preference: ThemePreference): ResolvedTheme {
  const resolved = resolveTheme(preference);
  applyResolvedTheme(resolved);
  return resolved;
}

/**
 * Inline script string injected into <head> before hydration.
 * Reads stored preference (or system query) and sets `data-theme` on <html>
 * synchronously, eliminating FOUC.
 */
export const THEME_INIT_SCRIPT = `(() => {
  try {
    var key = ${JSON.stringify(THEME_STORAGE_KEY)};
    var stored = null;
    try { stored = window.localStorage.getItem(key); } catch (_) {}
    var resolved = (stored === 'light' || stored === 'dark' || stored === 'paper') ? stored : null;
    if (!resolved) {
      var mq = window.matchMedia('(prefers-color-scheme: dark)');
      resolved = mq && mq.matches ? 'dark' : 'light';
    }
    document.documentElement.dataset.theme = resolved;
  } catch (_) {
    document.documentElement.dataset.theme = 'light';
  }
})();`;
