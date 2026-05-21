import type { Metadata } from 'next';
import { Fira_Code, Inter, Literata, Manrope, Roboto_Slab } from 'next/font/google';
import localFont from 'next/font/local';
import { ReadingPrefsProvider } from '@/components/ReadingPrefsProvider';
import { ThemeProvider } from '@/components/ThemeProvider';
import type { Course } from '@/lib/course';
import { loadCourse } from '@/lib/course-loader';
import { buildGateInitScript } from '@/lib/gate-init-script';
import { buildGateMarkScript } from '@/lib/gate-mark-script';
import { DEFAULT_LANG, LANGS, type Lang } from '@/lib/lang';
import { READING_PREFS_INIT_SCRIPT } from '@/lib/reading-prefs';
import { buildSiteUrl, getRuntimeBasePath, getSiteUrl } from '@/lib/site-url';
import { THEME_INIT_SCRIPT } from '@/lib/theme';
import '@/styles/globals.css';

const manrope = Manrope({
  subsets: ['latin', 'cyrillic'],
  display: 'swap',
  variable: '--font-ui',
});

// Self-hosted JetBrains Mono with full glyph coverage (incl. Box Drawing U+2500–U+257F),
// avoids Google Fonts' subset split that breaks ASCII-art alignment.
const jetbrains = localFont({
  variable: '--font-mono',
  display: 'swap',
  src: [
    { path: '../public/fonts/jetbrains-mono/JetBrainsMono-Regular.woff2', weight: '400', style: 'normal' },
    { path: '../public/fonts/jetbrains-mono/JetBrainsMono-Italic.woff2', weight: '400', style: 'italic' },
    { path: '../public/fonts/jetbrains-mono/JetBrainsMono-Medium.woff2', weight: '500', style: 'normal' },
    { path: '../public/fonts/jetbrains-mono/JetBrainsMono-SemiBold.woff2', weight: '600', style: 'normal' },
    { path: '../public/fonts/jetbrains-mono/JetBrainsMono-Bold.woff2', weight: '700', style: 'normal' },
    { path: '../public/fonts/jetbrains-mono/JetBrainsMono-BoldItalic.woff2', weight: '700', style: 'italic' },
  ],
});

// Literata is the default prose face — designed by TypeTogether for Google Play
// Books, tuned for long-form on-screen reading (low stroke contrast, sturdy
// serifs, near-upright italic that holds the pixel grid).
const literataProse = Literata({
  subsets: ['latin', 'cyrillic'],
  weight: ['400', '500', '600', '700'],
  style: ['normal', 'italic'],
  display: 'swap',
  variable: '--font-serif',
});

// Optional reading-prefs fonts. Weights are pinned explicitly so next/font does
// not pull the full axis (cyrillic subsets balloon otherwise). Only the CSS
// variable is exposed; the actual font kicks in once the user picks the
// matching prose/code option in <SettingsToggle>.
const interProse = Inter({
  subsets: ['latin', 'cyrillic'],
  weight: ['400', '500', '600', '700'],
  style: ['normal'],
  display: 'swap',
  variable: '--font-prose-inter',
});

// Roboto Slab provides the "slab" universe — third prose option. It has no
// true italic, so reserve it for accent/short-form blocks (see slab.css).
const robotoSlab = Roboto_Slab({
  subsets: ['latin', 'cyrillic'],
  weight: ['400', '500', '600', '700'],
  style: ['normal'],
  display: 'swap',
  variable: '--font-prose-slab',
});

const firaCode = Fira_Code({
  subsets: ['latin', 'cyrillic'],
  weight: ['400', '500', '700'],
  style: ['normal'],
  display: 'swap',
  variable: '--font-code-fira',
});

export function generateMetadata(): Metadata {
  // The root layout is the SEO entry point. Default-language content lives at
  // `/` (this layout's child), with `/{ru,en}/` mirrors under [lang]/. The
  // canonical points at the default-lang URL and `alternates.languages` lets
  // crawlers discover the per-lang copies.
  const course = loadCourse(DEFAULT_LANG);
  // course.yaml owns the per-lang description; loadCourse(DEFAULT_LANG) has
  // already resolved the right side of the `{ ru, en }` map. Normalize
  // whitespace so multi-line YAML scalars render as a single sentence.
  const description = course.description.replace(/\s+/g, ' ').trim();
  const canonical = buildSiteUrl(course.basePath, [DEFAULT_LANG]);
  const languages: Record<string, string> = {
    'x-default': canonical,
  };
  for (const lang of LANGS) {
    languages[lang] = buildSiteUrl(course.basePath, [lang]);
  }
  return {
    metadataBase: new URL(getSiteUrl()),
    title: course.title,
    description,
    alternates: { canonical, languages },
    openGraph: {
      type: 'website',
      siteName: course.title,
      title: course.title,
      description,
      url: canonical,
      locale: DEFAULT_LANG === 'ru' ? 'ru_RU' : 'en_US',
    },
    twitter: {
      card: 'summary_large_image',
      title: course.title,
      description,
    },
  };
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  // gate-init operates on lesson keys + linear order (language-agnostic), so
  // the default-lang course is enough. gate-mark, however, writes localized
  // lesson/module titles into CTA/hint slots — it must ship per-lang title
  // tables and pick the right one at runtime based on the URL prefix; without
  // this, RU pages flash EN titles into CTAs between SSR and hydration.
  const coursesByLang: Record<Lang, Course> = {
    ru: loadCourse('ru'),
    en: loadCourse('en'),
  };
  const course = coursesByLang[DEFAULT_LANG];
  const basePath = getRuntimeBasePath(course.basePath);
  const gateInitScript = buildGateInitScript(course, basePath);
  const gateMarkScript = buildGateMarkScript(coursesByLang, basePath, DEFAULT_LANG);
  return (
    <html
      lang={DEFAULT_LANG}
      data-theme="light"
      className={`${manrope.variable} ${jetbrains.variable} ${literataProse.variable} ${interProse.variable} ${robotoSlab.variable} ${firaCode.variable}`}
      suppressHydrationWarning
    >
      <head>
        <script
          id="theme-init"
          // FOUC-free: applies stored/system theme to <html data-theme> before hydration.
          dangerouslySetInnerHTML={{ __html: THEME_INIT_SCRIPT }}
        />
        <script
          id="reading-prefs-init"
          // FOUC-free: stamps four data-prose-*/data-code-* attributes on <html>
          // from localStorage before hydration. Sits between theme-init and
          // gate-init so personal-preferences scripts group together.
          dangerouslySetInnerHTML={{ __html: READING_PREFS_INIT_SCRIPT }}
        />
        <script
          id="gate-init"
          // Pre-hydration gate: stamps data-lesson-locked on <html> when the
          // current URL targets a locked lesson, so CSS can hide the content
          // before React mounts and there is no flash of "open" lesson body.
          // The script strips the `/{ru,en}/` prefix internally (see
          // gate-init-script.ts) so it stays lang-agnostic.
          dangerouslySetInnerHTML={{ __html: gateInitScript }}
        />
      </head>
      <body>
        <ThemeProvider>
          <ReadingPrefsProvider>{children}</ReadingPrefsProvider>
        </ThemeProvider>
        {/* Runs as the last body child — by that point every [data-lesson-key]
            element from server-rendered lists is in the DOM, so we can stamp
            data-locked before the browser paints. Stops the flash where rows
            momentarily appear unlocked before React's hydration cycle. */}
        <script
          id="gate-mark"
          dangerouslySetInnerHTML={{ __html: gateMarkScript }}
        />
      </body>
    </html>
  );
}
