import { describe, expect, it } from 'vitest';
import { LANGS } from './lang';
import { getDict, UI_STRINGS, type UIDict } from './i18n';

describe('UI_STRINGS', () => {
  it('has an entry for each Lang in LANGS', () => {
    for (const lang of LANGS) {
      expect(UI_STRINGS[lang]).toBeDefined();
    }
  });

  it('every dictionary has identical key sets', () => {
    const baseKeys = Object.keys(UI_STRINGS.ru).sort();
    for (const lang of LANGS) {
      expect(Object.keys(UI_STRINGS[lang]).sort()).toEqual(baseKeys);
    }
  });

  it('every value is a non-empty string', () => {
    for (const lang of LANGS) {
      const dict = UI_STRINGS[lang];
      for (const [key, value] of Object.entries(dict)) {
        expect(typeof value, `${lang}.${key} must be string`).toBe('string');
        expect(value.length, `${lang}.${key} must be non-empty`).toBeGreaterThan(0);
      }
    }
  });

  it('RU and EN values differ — translations are not stub copies', () => {
    const ru = UI_STRINGS.ru;
    const en = UI_STRINGS.en;
    const allowedEqual = new Set<keyof UIDict>([
      // Brand / proper nouns that stay the same across languages.
      'locked',
      'heroTitleLead',
      // CodeBlock "copy" label happens to read the same in both languages.
      'codeBlockCopy',
      // ReadingPrefs: button glyphs and font names are language-neutral.
      'readingPrefsDecrease',
      'readingPrefsIncrease',
      'readingPrefsFontSerif',
      'readingPrefsFontSans',
      'readingPrefsFontSlab',
      'readingPrefsFontJetBrains',
      'readingPrefsFontFira',
      // SettingsToggle eyebrow is a code-style path that stays the same.
      'settingsEyebrow',
      // Code preview is a literal code snippet — same in both languages.
      'readingPrefsPreviewCode',
    ]);
    for (const key of Object.keys(ru) as (keyof UIDict)[]) {
      if (allowedEqual.has(key)) continue;
      expect(ru[key], `key "${key}" must differ between RU and EN`).not.toBe(en[key]);
    }
  });
});

describe('getDict', () => {
  it('returns the RU dictionary for ru', () => {
    expect(getDict('ru')).toBe(UI_STRINGS.ru);
  });

  it('returns the EN dictionary for en', () => {
    expect(getDict('en')).toBe(UI_STRINGS.en);
  });

  it('returned dict has all UIDict keys', () => {
    const dict = getDict('en');
    expect(dict.home).toBeDefined();
    expect(dict.programCourse).toBeDefined();
    expect(dict.notFoundTitle).toBeDefined();
    expect(dict.themeLight).toBeDefined();
    expect(dict.language).toBeDefined();
  });
});
