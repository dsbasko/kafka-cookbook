import { type Lang } from './lang';

/**
 * UI string dictionary. Every key here must exist in every entry of
 * UI_STRINGS — TypeScript enforces this via the `Record<Lang, UIDict>`
 * shape below. Add new keys here first, then translations.
 *
 * Scope: strings rendered by components listed in Task 10 of the i18n
 * plan (Header/Breadcrumbs, Sidebar, ProgramDrawer, LessonNav, Toc,
 * ReadingProgress, LessonLockedInterstitial, SettingsToggle, HomePage,
 * LessonPageLayout, not-found) plus a few keys for adjacent surfaces
 * already touched by lang switching (settings popover, theme labels
 * previously lived in `lib/theme.ts`).
 */
export type UIDict = {
  // Sidebar
  sidebarLabel: string;
  navMainLabel: string;
  home: string;
  programCourse: string;
  githubRepo: string;

  // Header / Breadcrumbs
  breadcrumbsLabel: string;

  // ProgramDrawer
  close: string;
  moduleListLabel: string;
  lessonLockTitle: string;

  // LessonNav
  lessonNavLabel: string;
  prevLesson: string;
  nextLesson: string;

  // HeaderLessonNav (chevron buttons reuse a shorter form without the arrow)
  prevLessonAria: string;
  nextLessonAria: string;
  firstLessonTitle: string;
  lastLessonTitle: string;

  // Toc
  tocLabel: string;

  // ReadingProgress
  readingProgressLabel: string;

  // LessonLockedInterstitial
  locked: string;
  moduleNumberPrefix: string;
  lockedTitle: string;
  lockedDesc: string;
  attemptedLessonLabel: string;
  attemptedYouTried: string;
  continueAction: string;
  startFromFirst: string;
  openProgram: string;
  courseProgress: string;
  progress: string;
  nextStep: string;
  untilThisLesson: string;

  // SettingsToggle — theme section labels
  themeLight: string;
  themeDark: string;
  themePaper: string;

  // HomePage
  heroTitleLead: string;
  heroTitleAccent: string;
  heroTitleTail: string;
  continueLessonPrefix: string;
  startFromScratch: string;
  progressSummary: string;
  modulesLabel: string;
  lessonsLabel: string;
  durationLabel: string;
  stackLabel: string;
  moduleLockTitle: string;

  // LessonPageLayout
  lessonInfoLabel: string;

  // not-found
  notFoundTitle: string;
  notFoundDesc: string;
  goHome: string;

  // SettingsToggle — language section title (also used as radiogroup aria-label)
  language: string;

  // TranslationBanner (rendered on EN lesson pages that fall back to RU)
  translationFallbackTitle: string;
  translationFallbackBody: string;

  // ModulePage
  startModule: string;
  continueModulePrefix: string;
  rereadModule: string;
  nextModule: string;
  moduleProgress: string;
  lessonsCount: string;
  durationLabelShort: string;
  stackLabelShort: string;
  moduleLessonsHeading: string;
  lessonLockShort: string;
  lessonHintContinue: string;
  lessonNeighbourModulesLabel: string;
  prevModule: string;

  // LessonSideMeta
  moduleMetaKey: string;
  readingTimeMetaKey: string;
  tagsMetaKey: string;
  markUnread: string;

  // ProgressBar / aria
  progressBarAriaLabel: string;
  progressAriaConnector: string;

  // Metadata fallbacks
  notFoundMetadataTitle: string;
  ogImageAlt: string;

  // HomePage module status (CSS-only pseudo-element labels lifted into JSX)
  statusNotStarted: string;
  statusInProgress: string;
  statusComplete: string;

  // Callout titles (rendered server-side via MDX pipeline)
  calloutNote: string;
  calloutTip: string;
  calloutWarning: string;
  calloutImportant: string;
  calloutCaution: string;

  // CodeBlock copy button
  codeBlockCopy: string;
  codeBlockCopied: string;
  codeBlockCopyAriaLabel: string;
  codeBlockCopiedAriaLabel: string;

  // SettingsToggle — trigger label and section titles
  settingsLabel: string;
  settingsEyebrow: string;
  settingsThemeSection: string;

  // SettingsToggle — prose/code sections
  readingPrefsProseSection: string;
  readingPrefsCodeSection: string;
  readingPrefsSize: string;
  readingPrefsFont: string;
  readingPrefsDecrease: string;
  readingPrefsIncrease: string;
  readingPrefsFontSerif: string;
  readingPrefsFontSans: string;
  readingPrefsFontSlab: string;
  readingPrefsFontJetBrains: string;
  readingPrefsFontFira: string;
  readingPrefsPreviewProse: string;
  readingPrefsPreviewCode: string;
};

export const UI_STRINGS: Record<Lang, UIDict> = {
  ru: {
    sidebarLabel: 'Боковая навигация',
    navMainLabel: 'Основная навигация',
    home: 'Главная',
    programCourse: 'Программа курса',
    githubRepo: 'Репозиторий на GitHub',

    breadcrumbsLabel: 'Хлебные крошки',

    close: 'Закрыть',
    moduleListLabel: 'Список модулей и уроков',
    lessonLockTitle: 'Урок откроется после прохождения предыдущих',

    lessonNavLabel: 'Навигация по урокам',
    prevLesson: '← Предыдущий урок',
    nextLesson: 'Следующий урок →',

    prevLessonAria: 'Предыдущий урок',
    nextLessonAria: 'Следующий урок',
    firstLessonTitle: 'Это первый урок',
    lastLessonTitle: 'Это последний урок',

    tocLabel: 'Содержание',

    readingProgressLabel: 'Прогресс чтения',

    locked: 'LOCKED',
    moduleNumberPrefix: 'Модуль',
    lockedTitle: 'Этот урок ещё впереди',
    lockedDesc:
      'Курс изучается по порядку — чтобы открыть этот шаг, сначала завершите предыдущие. Так контекст накапливается без пропусков.',
    attemptedLessonLabel: 'Урок, который вы открыли',
    attemptedYouTried: '/ вы пытались открыть',
    continueAction: 'Продолжить',
    startFromFirst: 'Начать с первого урока',
    openProgram: 'Открыть программу',
    courseProgress: 'Прогресс курса',
    progress: 'Прогресс',
    nextStep: 'Следующий шаг',
    untilThisLesson: 'До этого урока',

    themeLight: 'Светлая',
    themeDark: 'Тёмная',
    themePaper: 'Бумага',

    heroTitleLead: 'Kafka',
    heroTitleAccent: 'для тех, кто',
    heroTitleTail: 'пишет на Go',
    continueLessonPrefix: 'Продолжить · урок',
    startFromScratch: 'Начать с начала',
    progressSummary: 'Сводка прогресса',
    modulesLabel: 'Модулей',
    lessonsLabel: 'Уроков',
    durationLabel: 'Длительность',
    stackLabel: 'Стек',
    moduleLockTitle: 'Модуль откроется после прохождения предыдущих уроков',

    lessonInfoLabel: 'Сведения об уроке',

    notFoundTitle: 'Страница не найдена',
    notFoundDesc: 'Похоже, такой лекции в курсе нет. Вернитесь на главную.',
    goHome: 'На главную',

    language: 'Язык',

    translationFallbackTitle: 'Перевод в процессе',
    translationFallbackBody:
      'Английская версия этой лекции пока не готова. Показан оригинал на русском.',

    startModule: 'Начать модуль',
    continueModulePrefix: 'Продолжить',
    rereadModule: 'Перечитать модуль',
    nextModule: 'Следующий модуль',
    moduleProgress: 'Прогресс модуля',
    lessonsCount: 'Уроков',
    durationLabelShort: 'Длительность',
    stackLabelShort: 'Стек',
    moduleLessonsHeading: 'Уроки модуля',
    lessonLockShort: 'Урок откроется после прохождения предыдущих',
    lessonHintContinue: '↳ продолжить отсюда',
    lessonNeighbourModulesLabel: 'Соседние модули',
    prevModule: '← Предыдущий модуль',

    moduleMetaKey: 'модуль',
    readingTimeMetaKey: 'время чтения',
    tagsMetaKey: 'теги',
    markUnread: 'Пометить непрочитанным',

    progressBarAriaLabel: 'Прогресс прохождения курса',
    progressAriaConnector: 'из',

    notFoundMetadataTitle: 'Страница не найдена · Kafka Cookbook',
    ogImageAlt: 'Kafka Cookbook — курс по Apache Kafka на Go',

    statusNotStarted: 'не начато',
    statusInProgress: 'в процессе',
    statusComplete: 'пройдено',

    calloutNote: 'Заметка',
    calloutTip: 'Подсказка',
    calloutWarning: 'Внимание',
    calloutImportant: 'Важно',
    calloutCaution: 'Осторожно',

    codeBlockCopy: 'copy',
    codeBlockCopied: '✓ скопировано',
    codeBlockCopyAriaLabel: 'Скопировать код',
    codeBlockCopiedAriaLabel: 'Скопировано',

    settingsLabel: 'Настройки',
    settingsEyebrow: '/ config',
    settingsThemeSection: 'Тема',
    readingPrefsProseSection: 'Текст лекции',
    readingPrefsCodeSection: 'Текст кода',
    readingPrefsSize: 'Размер',
    readingPrefsFont: 'Шрифт',
    readingPrefsDecrease: 'A−',
    readingPrefsIncrease: 'A+',
    readingPrefsFontSerif: 'Literata',
    readingPrefsFontSans: 'Inter',
    readingPrefsFontSlab: 'Roboto Slab',
    readingPrefsFontJetBrains: 'JetBrains',
    readingPrefsFontFira: 'Fira',
    readingPrefsPreviewProse:
      'Apache Kafka — распределённая платформа потоковой передачи событий.',
    readingPrefsPreviewCode: 'consumer.subscribe(topics)',
  },
  en: {
    sidebarLabel: 'Side navigation',
    navMainLabel: 'Main navigation',
    home: 'Home',
    programCourse: 'Course outline',
    githubRepo: 'GitHub repository',

    breadcrumbsLabel: 'Breadcrumbs',

    close: 'Close',
    moduleListLabel: 'Module and lesson list',
    lessonLockTitle: 'Lesson unlocks after the previous ones are complete',

    lessonNavLabel: 'Lesson navigation',
    prevLesson: '← Previous lesson',
    nextLesson: 'Next lesson →',

    prevLessonAria: 'Previous lesson',
    nextLessonAria: 'Next lesson',
    firstLessonTitle: 'This is the first lesson',
    lastLessonTitle: 'This is the last lesson',

    tocLabel: 'Contents',

    readingProgressLabel: 'Reading progress',

    locked: 'LOCKED',
    moduleNumberPrefix: 'Module',
    lockedTitle: 'This lesson is still ahead',
    lockedDesc:
      'The course goes in order — to open this step, finish the previous ones first. Context builds up without gaps that way.',
    attemptedLessonLabel: 'Lesson you tried to open',
    attemptedYouTried: '/ you tried to open',
    continueAction: 'Continue',
    startFromFirst: 'Start from the first lesson',
    openProgram: 'Open outline',
    courseProgress: 'Course progress',
    progress: 'Progress',
    nextStep: 'Next step',
    untilThisLesson: 'Until this lesson',

    themeLight: 'Light',
    themeDark: 'Dark',
    themePaper: 'Paper',

    heroTitleLead: 'Kafka',
    heroTitleAccent: 'for people who',
    heroTitleTail: 'write Go',
    continueLessonPrefix: 'Continue · lesson',
    startFromScratch: 'Start over',
    progressSummary: 'Progress summary',
    modulesLabel: 'Modules',
    lessonsLabel: 'Lessons',
    durationLabel: 'Duration',
    stackLabel: 'Stack',
    moduleLockTitle: 'Module unlocks after the previous lessons are complete',

    lessonInfoLabel: 'Lesson info',

    notFoundTitle: 'Page not found',
    notFoundDesc: 'There is no such lesson in the course. Head back home.',
    goHome: 'Go home',

    language: 'Language',

    translationFallbackTitle: 'Translation in progress',
    translationFallbackBody:
      'The English version of this lesson is not ready yet. Showing the original Russian text.',

    startModule: 'Start module',
    continueModulePrefix: 'Continue',
    rereadModule: 'Reread module',
    nextModule: 'Next module',
    moduleProgress: 'Module progress',
    lessonsCount: 'Lessons',
    durationLabelShort: 'Duration',
    stackLabelShort: 'Stack',
    moduleLessonsHeading: 'Lessons',
    lessonLockShort: 'Lesson unlocks after the previous ones are complete',
    lessonHintContinue: '↳ continue from here',
    lessonNeighbourModulesLabel: 'Neighbouring modules',
    prevModule: '← Previous module',

    moduleMetaKey: 'module',
    readingTimeMetaKey: 'reading time',
    tagsMetaKey: 'tags',
    markUnread: 'Mark as unread',

    progressBarAriaLabel: 'Course completion progress',
    progressAriaConnector: 'of',

    notFoundMetadataTitle: 'Page not found · Kafka Cookbook',
    ogImageAlt: 'Kafka Cookbook — Apache Kafka course in Go',

    statusNotStarted: 'not started',
    statusInProgress: 'in progress',
    statusComplete: 'complete',

    calloutNote: 'Note',
    calloutTip: 'Tip',
    calloutWarning: 'Warning',
    calloutImportant: 'Important',
    calloutCaution: 'Caution',

    codeBlockCopy: 'copy',
    codeBlockCopied: '✓ copied',
    codeBlockCopyAriaLabel: 'Copy code',
    codeBlockCopiedAriaLabel: 'Copied',

    settingsLabel: 'Settings',
    settingsEyebrow: '/ config',
    settingsThemeSection: 'Theme',
    readingPrefsProseSection: 'Lesson text',
    readingPrefsCodeSection: 'Code text',
    readingPrefsSize: 'Size',
    readingPrefsFont: 'Font',
    readingPrefsDecrease: 'A−',
    readingPrefsIncrease: 'A+',
    readingPrefsFontSerif: 'Literata',
    readingPrefsFontSans: 'Inter',
    readingPrefsFontSlab: 'Roboto Slab',
    readingPrefsFontJetBrains: 'JetBrains',
    readingPrefsFontFira: 'Fira',
    readingPrefsPreviewProse: 'Apache Kafka is a distributed event streaming platform.',
    readingPrefsPreviewCode: 'consumer.subscribe(topics)',
  },
};

/**
 * Server-friendly accessor. Server components receive `lang` via route
 * params; pass it in here. Client components should prefer `useT()` from
 * `web/lib/use-i18n.ts` instead — it reads `lang` from `useParams()`.
 */
export function getDict(lang: Lang): UIDict {
  return UI_STRINGS[lang];
}
