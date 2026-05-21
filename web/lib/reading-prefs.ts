export type SizeStep = 0 | 1 | 2 | 3;
export type ProseFont = 'serif' | 'sans' | 'slab';
export type CodeFont = 'jetbrains' | 'fira';

export interface ReadingPrefs {
  proseSize: SizeStep;
  codeSize: SizeStep;
  proseFont: ProseFont;
  codeFont: CodeFont;
}

// Bumped from v1 → v2 when the prose `lora` option became `slab` (Roboto Slab)
// and the `plex` code option was retired. Old stored values fall back to defaults.
export const READING_PREFS_STORAGE_KEY = 'kafka-cookbook-reading-prefs:v2';

export const PROSE_FONTS = ['serif', 'sans', 'slab'] as const;
export const CODE_FONTS = ['jetbrains', 'fira'] as const;
export const SIZE_STEPS = [0, 1, 2, 3] as const;

// Step values match design reference: 14 / 16 / 18 / 20 px.
export const DEFAULT_PREFS: ReadingPrefs = {
  proseSize: 1,
  codeSize: 0,
  proseFont: 'serif',
  codeFont: 'jetbrains',
};

export function isProseFont(value: unknown): value is ProseFont {
  return value === 'serif' || value === 'sans' || value === 'slab';
}

export function isCodeFont(value: unknown): value is CodeFont {
  return value === 'jetbrains' || value === 'fira';
}

export function isSizeStep(value: unknown): value is SizeStep {
  return value === 0 || value === 1 || value === 2 || value === 3;
}

export function readStoredPrefs(): ReadingPrefs {
  if (typeof window === 'undefined') return { ...DEFAULT_PREFS };
  let raw: string | null = null;
  try {
    raw = window.localStorage.getItem(READING_PREFS_STORAGE_KEY);
  } catch {
    return { ...DEFAULT_PREFS };
  }
  if (raw == null) return { ...DEFAULT_PREFS };
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return { ...DEFAULT_PREFS };
  }
  if (!parsed || typeof parsed !== 'object') return { ...DEFAULT_PREFS };
  const obj = parsed as Record<string, unknown>;
  return {
    proseSize: isSizeStep(obj.proseSize) ? obj.proseSize : DEFAULT_PREFS.proseSize,
    codeSize: isSizeStep(obj.codeSize) ? obj.codeSize : DEFAULT_PREFS.codeSize,
    proseFont: isProseFont(obj.proseFont) ? obj.proseFont : DEFAULT_PREFS.proseFont,
    codeFont: isCodeFont(obj.codeFont) ? obj.codeFont : DEFAULT_PREFS.codeFont,
  };
}

export function writeStoredPrefs(prefs: ReadingPrefs): void {
  if (typeof window === 'undefined') return;
  try {
    window.localStorage.setItem(READING_PREFS_STORAGE_KEY, JSON.stringify(prefs));
  } catch {
    /* storage may be unavailable (private mode, quota); ignore. */
  }
}

export function applyPrefs(prefs: ReadingPrefs): void {
  if (typeof document === 'undefined') return;
  const ds = document.documentElement.dataset;
  ds.proseSize = String(prefs.proseSize);
  ds.codeSize = String(prefs.codeSize);
  ds.proseFont = prefs.proseFont;
  ds.codeFont = prefs.codeFont;
}

/**
 * Inline script injected into <head> before hydration. Reads localStorage,
 * validates per-field, and stamps four data-* attributes on <html>
 * synchronously to avoid FOUC.
 */
export const READING_PREFS_INIT_SCRIPT = `(() => {
  var KEY = ${JSON.stringify(READING_PREFS_STORAGE_KEY)};
  var defaults = ${JSON.stringify(DEFAULT_PREFS)};
  var sizes = [0, 1, 2, 3];
  var proseFonts = ['serif', 'sans', 'slab'];
  var codeFonts = ['jetbrains', 'fira'];
  var prefs = {
    proseSize: defaults.proseSize,
    codeSize: defaults.codeSize,
    proseFont: defaults.proseFont,
    codeFont: defaults.codeFont,
  };
  try {
    var raw = null;
    try { raw = window.localStorage.getItem(KEY); } catch (_) {}
    if (raw != null) {
      var parsed = JSON.parse(raw);
      if (parsed && typeof parsed === 'object') {
        if (sizes.indexOf(parsed.proseSize) !== -1) prefs.proseSize = parsed.proseSize;
        if (sizes.indexOf(parsed.codeSize) !== -1) prefs.codeSize = parsed.codeSize;
        if (proseFonts.indexOf(parsed.proseFont) !== -1) prefs.proseFont = parsed.proseFont;
        if (codeFonts.indexOf(parsed.codeFont) !== -1) prefs.codeFont = parsed.codeFont;
      }
    }
  } catch (_) {}
  try {
    var ds = document.documentElement.dataset;
    ds.proseSize = String(prefs.proseSize);
    ds.codeSize = String(prefs.codeSize);
    ds.proseFont = prefs.proseFont;
    ds.codeFont = prefs.codeFont;
  } catch (_) {}
})();`;
