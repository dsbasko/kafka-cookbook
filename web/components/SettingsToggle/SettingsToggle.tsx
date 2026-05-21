'use client';

import { useEffect, useId, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { usePathname, useRouter } from 'next/navigation';
import { SettingsIcon } from '@/components/Sidebar/icons';
import { useReadingPrefs } from '@/components/ReadingPrefsProvider';
import { useTheme } from '@/components/ThemeProvider';
import {
  CODE_FONTS,
  PROSE_FONTS,
  type CodeFont,
  type ProseFont,
  type SizeStep,
} from '@/lib/reading-prefs';
import {
  LANG_LABELS,
  LANGS,
  stripLangFromPath,
  writeStoredLang,
  type Lang,
} from '@/lib/lang';
import { THEME_PREFERENCES, type ThemePreference } from '@/lib/theme';
import { useT } from '@/lib/use-i18n';
import type { UIDict } from '@/lib/i18n';
import styles from './SettingsToggle.module.css';

const SETTINGS_REOPEN_KEY = 'kafka-cookbook:settings:reopen-after-lang';

const SIZE_STEPS: SizeStep[] = [0, 1, 2, 3, 4, 5];
const MAX_SIZE_STEP: SizeStep = 5;

const PROSE_SIZE_PX: Record<SizeStep, number> = {
  0: 14,
  1: 16,
  2: 18,
  3: 20,
  4: 22,
  5: 24,
};

const CODE_SIZE_PX: Record<SizeStep, number> = {
  0: 14,
  1: 16,
  2: 18,
  3: 20,
  4: 22,
  5: 24,
};

const PROSE_FONT_LABEL_KEYS: Record<ProseFont, keyof UIDict> = {
  serif: 'readingPrefsFontSerif',
  sans: 'readingPrefsFontSans',
  slab: 'readingPrefsFontSlab',
};

const CODE_FONT_LABEL_KEYS: Record<CodeFont, keyof UIDict> = {
  jetbrains: 'readingPrefsFontJetBrains',
  fira: 'readingPrefsFontFira',
};

const THEME_LABEL_KEYS: Record<
  ThemePreference,
  'themeLight' | 'themeDark' | 'themePaper'
> = {
  light: 'themeLight',
  dark: 'themeDark',
  paper: 'themePaper',
};

function stepDown(step: SizeStep): SizeStep {
  return (step > 0 ? step - 1 : 0) as SizeStep;
}

function stepUp(step: SizeStep): SizeStep {
  return (step < MAX_SIZE_STEP ? step + 1 : MAX_SIZE_STEP) as SizeStep;
}

function replaceLangInPath(pathname: string | null, next: Lang): string {
  const { rest } = stripLangFromPath(pathname ?? '/');
  if (rest === '/' || rest === '') return `/${next}/`;
  return `/${next}${rest}`;
}

function CloseIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" aria-hidden="true">
      <path d="M6 6l12 12M18 6 6 18" />
    </svg>
  );
}

function LightThemeIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <circle cx="8" cy="8" r="2.6" />
      <path d="M8 1.6v1.6M8 12.8v1.6M1.6 8h1.6M12.8 8h1.6M3.5 3.5l1.1 1.1M11.4 11.4l1.1 1.1M3.5 12.5l1.1-1.1M11.4 4.6l1.1-1.1" />
    </svg>
  );
}

function PaperThemeIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <path d="M3.5 2.5h6.4l2.6 2.6V13a.5.5 0 0 1-.5.5h-8.5a.5.5 0 0 1-.5-.5V3a.5.5 0 0 1 .5-.5z" />
      <path d="M9.6 2.6V5.2h2.6" />
      <path d="M5.5 8.4h5M5.5 10.6h3.6" />
    </svg>
  );
}

function DarkThemeIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <path d="M13.2 9.6A5.6 5.6 0 1 1 6.4 2.8a4.6 4.6 0 0 0 6.8 6.8z" />
    </svg>
  );
}

const THEME_ICONS: Record<ThemePreference, () => JSX.Element> = {
  light: LightThemeIcon,
  paper: PaperThemeIcon,
  dark: DarkThemeIcon,
};

type SizeStepperProps = {
  kind: 'prose' | 'code';
  value: SizeStep;
  pxLabel: string;
  onStep: (next: SizeStep) => void;
  decreaseLabel: string;
  increaseLabel: string;
};

function SizeStepper({ kind, value, pxLabel, onStep, decreaseLabel, increaseLabel }: SizeStepperProps) {
  const isMin = value === 0;
  const isMax = value === MAX_SIZE_STEP;
  const percent = (value / MAX_SIZE_STEP) * 100;
  return (
    <div className={styles.sizeStepper}>
      <button
        type="button"
        className={styles.sizeBtn}
        aria-label={decreaseLabel}
        disabled={isMin}
        onClick={() => onStep(stepDown(value))}
        data-kind={`${kind}-decrease`}
      >
        <span className={styles.sizeBtnGlyphSmall}>A</span>
      </button>
      <div className={styles.sizeTrack}>
        <div className={styles.sizeRail} />
        <div className={styles.sizeRailFill} style={{ width: `${percent}%` }} />
        <div className={styles.sizeKnob} style={{ left: `${percent}%` }} />
        <div className={styles.sizeTicks}>
          {SIZE_STEPS.map((step) => (
            <button
              key={step}
              type="button"
              className={styles.sizeTick}
              data-active={step === value ? 'true' : 'false'}
              onClick={() => onStep(step)}
              aria-label={`${kind === 'prose' ? PROSE_SIZE_PX[step] : CODE_SIZE_PX[step]}px`}
            >
              <span className={styles.sizeTickDot} />
            </button>
          ))}
        </div>
      </div>
      <button
        type="button"
        className={styles.sizeBtn}
        aria-label={increaseLabel}
        disabled={isMax}
        onClick={() => onStep(stepUp(value))}
        data-kind={`${kind}-increase`}
      >
        <span className={styles.sizeBtnGlyphLarge}>A</span>
      </button>
      <span className={styles.sizeValue} aria-live="polite" data-kind={`${kind}-value`}>
        {pxLabel.replace(/px\s*$/i, '')}
        <span className={styles.sizeUnit}>px</span>
      </span>
    </div>
  );
}

export function SettingsToggle() {
  const t = useT();
  const { prefs, setProseSize, setCodeSize, setProseFont, setCodeFont } = useReadingPrefs();
  const { preference: themePreference, setPreference: setThemePreference } = useTheme();
  const router = useRouter();
  const pathname = usePathname();
  const currentLang = stripLangFromPath(pathname ?? '/').lang;

  const [open, setOpen] = useState(false);
  const [renderedSizes, setRenderedSizes] = useState<{ prose: string; code: string } | null>(null);
  const [isMobile, setIsMobile] = useState(false);
  const [mounted, setMounted] = useState(false);
  const wrapperRef = useRef<HTMLDivElement>(null);
  const popoverRef = useRef<HTMLDivElement>(null);
  const popoverId = useId();

  useEffect(() => {
    setMounted(true);
    // Reopen after a language switch — changing [lang] segment remounts
    // the layout (and this component), which would otherwise drop `open`.
    try {
      if (sessionStorage.getItem(SETTINGS_REOPEN_KEY) === '1') {
        sessionStorage.removeItem(SETTINGS_REOPEN_KEY);
        setOpen(true);
      }
    } catch {
      /* sessionStorage unavailable; ignore. */
    }
  }, []);

  useEffect(() => {
    if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') return;
    const mq = window.matchMedia('(max-width: 1023px)');
    const sync = () => setIsMobile(mq.matches);
    sync();
    mq.addEventListener('change', sync);
    return () => mq.removeEventListener('change', sync);
  }, []);

  useEffect(() => {
    if (!open) return;
    function handlePointer(event: MouseEvent) {
      const target = event.target as Node | null;
      if (!target) return;
      if (wrapperRef.current?.contains(target)) return;
      if (popoverRef.current?.contains(target)) return;
      setOpen(false);
    }
    function handleKey(event: KeyboardEvent) {
      if (event.key === 'Escape') setOpen(false);
    }
    document.addEventListener('mousedown', handlePointer);
    document.addEventListener('keydown', handleKey);
    return () => {
      document.removeEventListener('mousedown', handlePointer);
      document.removeEventListener('keydown', handleKey);
    };
  }, [open]);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    function readActual() {
      const cs = window.getComputedStyle(document.documentElement);
      const prose = cs.getPropertyValue('--prose-font-size').trim();
      const code = cs.getPropertyValue('--code-font-size').trim();
      if (prose && code) setRenderedSizes({ prose, code });
      else setRenderedSizes(null);
    }
    readActual();
    if (typeof window.matchMedia !== 'function') return;
    const mq = window.matchMedia('(max-width: 720px)');
    mq.addEventListener('change', readActual);
    return () => mq.removeEventListener('change', readActual);
  }, [prefs.proseSize, prefs.codeSize]);

  function handleLangSelect(next: Lang) {
    writeStoredLang(next);
    if (next === currentLang) return;
    try {
      sessionStorage.setItem(SETTINGS_REOPEN_KEY, '1');
    } catch {
      /* sessionStorage unavailable; popup will simply close. */
    }
    router.push(replaceLangInPath(pathname, next));
  }

  const proseSizeLabel = renderedSizes?.prose ?? `${PROSE_SIZE_PX[prefs.proseSize]}px`;
  const codeSizeLabel = renderedSizes?.code ?? `${CODE_SIZE_PX[prefs.codeSize]}px`;

  const overlayAndPopover = (
    <>
      {open && (
        <button
          type="button"
          className={styles.overlay}
          aria-label={t.close}
          onClick={() => setOpen(false)}
        />
      )}
      <div
        ref={popoverRef}
        id={popoverId}
        role="dialog"
        aria-label={t.settingsLabel}
        className={styles.popover}
        data-open={open ? 'true' : 'false'}
        hidden={!open}
      >
        <div className={styles.grabber} aria-hidden="true" />
        <header className={styles.head}>
          <div className={styles.headTitles}>
            <div className={styles.eyebrow}>{t.settingsEyebrow}</div>
            <h2 className={styles.title}>{t.settingsLabel}</h2>
          </div>
          <button
            type="button"
            className={styles.closeBtn}
            onClick={() => setOpen(false)}
            aria-label={t.close}
          >
            <CloseIcon />
          </button>
        </header>

        <div className={styles.body}>
          <div className={styles.row}>
            <span className={styles.rowLabel}>{t.settingsThemeSection}</span>
            <div className={styles.seg} role="radiogroup" aria-label={t.settingsThemeSection}>
              {THEME_PREFERENCES.map((value) => {
                const active = themePreference === value;
                const label = t[THEME_LABEL_KEYS[value]];
                const Icon = THEME_ICONS[value];
                return (
                  <button
                    key={value}
                    type="button"
                    role="radio"
                    aria-checked={active}
                    aria-label={label}
                    className={`${styles.segOpt} ${styles.segTheme}`}
                    data-active={active ? 'true' : 'false'}
                    data-theme-pref={value}
                    onClick={() => setThemePreference(value)}
                  >
                    <span className={styles.segThemeIcon} aria-hidden="true">
                      <Icon />
                    </span>
                    <span>{label}</span>
                  </button>
                );
              })}
            </div>
          </div>

          <div className={styles.row}>
            <span className={styles.rowLabel}>{t.language}</span>
            <div className={styles.seg} role="radiogroup" aria-label={t.language}>
              {LANGS.map((value) => {
                const active = currentLang === value;
                const label = LANG_LABELS[value];
                return (
                  <button
                    key={value}
                    type="button"
                    role="radio"
                    aria-checked={active}
                    aria-label={label}
                    className={`${styles.segOpt} ${styles.segLang}`}
                    data-active={active ? 'true' : 'false'}
                    data-lang-pref={value}
                    onClick={() => handleLangSelect(value)}
                  >
                    <span className={styles.segLangBadge}>{value.toUpperCase()}</span>
                    <span>{label}</span>
                  </button>
                );
              })}
            </div>
          </div>

          <div className={styles.divider} />

          <section className={styles.group}>
            <h3 className={styles.groupTitle}>{t.readingPrefsProseSection}</h3>
            <div
              className={styles.preview}
              style={{
                fontFamily: 'var(--prose-font-active, var(--font-serif))',
                fontSize: 'var(--prose-font-size, 16px)',
              }}
            >
              {t.readingPrefsPreviewProse}
            </div>

            <div className={styles.row}>
              <span className={styles.rowLabel}>{t.readingPrefsFont}</span>
              <div
                className={styles.seg}
                role="radiogroup"
                aria-label={`${t.readingPrefsProseSection} — ${t.readingPrefsFont}`}
              >
                {PROSE_FONTS.map((value) => {
                  const active = prefs.proseFont === value;
                  const label = t[PROSE_FONT_LABEL_KEYS[value]];
                  return (
                    <button
                      key={value}
                      type="button"
                      role="radio"
                      aria-checked={active}
                      aria-label={label}
                      className={styles.segOpt}
                      data-active={active ? 'true' : 'false'}
                      data-prose-font={value}
                      onClick={() => setProseFont(value)}
                    >
                      {label}
                    </button>
                  );
                })}
              </div>
            </div>

            <div className={styles.row}>
              <span className={styles.rowLabel}>{t.readingPrefsSize}</span>
              <SizeStepper
                kind="prose"
                value={prefs.proseSize}
                pxLabel={proseSizeLabel}
                onStep={setProseSize}
                decreaseLabel={t.readingPrefsDecrease}
                increaseLabel={t.readingPrefsIncrease}
              />
            </div>
          </section>

          <div className={styles.divider} />

          <section className={styles.group}>
            <h3 className={styles.groupTitle}>{t.readingPrefsCodeSection}</h3>
            <div
              className={`${styles.preview} ${styles.previewCode}`}
              style={{
                fontFamily: 'var(--code-font-active, var(--font-mono))',
                fontSize: 'var(--code-font-size, 14px)',
              }}
            >
              {t.readingPrefsPreviewCode}
            </div>

            <div className={styles.row}>
              <span className={styles.rowLabel}>{t.readingPrefsFont}</span>
              <div
                className={styles.seg}
                role="radiogroup"
                aria-label={`${t.readingPrefsCodeSection} — ${t.readingPrefsFont}`}
              >
                {CODE_FONTS.map((value) => {
                  const active = prefs.codeFont === value;
                  const label = t[CODE_FONT_LABEL_KEYS[value]];
                  return (
                    <button
                      key={value}
                      type="button"
                      role="radio"
                      aria-checked={active}
                      aria-label={label}
                      className={`${styles.segOpt} ${styles.segMono}`}
                      data-active={active ? 'true' : 'false'}
                      data-code-font={value}
                      onClick={() => setCodeFont(value)}
                    >
                      {label}
                    </button>
                  );
                })}
              </div>
            </div>

            <div className={styles.row}>
              <span className={styles.rowLabel}>{t.readingPrefsSize}</span>
              <SizeStepper
                kind="code"
                value={prefs.codeSize}
                pxLabel={codeSizeLabel}
                onStep={setCodeSize}
                decreaseLabel={t.readingPrefsDecrease}
                increaseLabel={t.readingPrefsIncrease}
              />
            </div>
          </section>
        </div>
      </div>
    </>
  );

  return (
    <div className={styles.wrapper} ref={wrapperRef}>
      <button
        type="button"
        className={styles.trigger}
        aria-label={t.settingsLabel}
        title={t.settingsLabel}
        aria-haspopup="dialog"
        aria-expanded={open}
        aria-controls={popoverId}
        onClick={() => setOpen((prev) => !prev)}
      >
        <SettingsIcon />
      </button>
      {mounted && isMobile
        ? createPortal(overlayAndPopover, document.body)
        : overlayAndPopover}
    </div>
  );
}
