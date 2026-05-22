'use client';

import { useEffect, useState, type ReactNode } from 'react';
import { usePathname } from 'next/navigation';
import { Sidebar } from '@/components/Sidebar';
import { Header } from '@/components/Header';
import { Breadcrumbs } from '@/components/Header/Breadcrumbs';
import { HeaderLessonNav } from '@/components/Header/HeaderLessonNav';
import { ProgramDrawer } from '@/components/ProgramDrawer';
import { ProgressBar } from '@/components/ProgressBar';
import {
  type Course,
  getNextLesson,
  getPrevLesson,
  getTotalLessons,
} from '@/lib/course';
import { DEFAULT_LANG, stripLangFromPath } from '@/lib/lang';
import { OPEN_PROGRAM_EVENT } from '@/lib/program-drawer';
import styles from './AppShell.module.css';

type AppShellProps = {
  children: ReactNode;
  course: Course;
};

export function AppShell({ children, course }: AppShellProps) {
  const pathname = usePathname() ?? '/';
  const [isDrawerOpen, setIsDrawerOpen] = useState(false);

  // Allow descendant components (Hero CTA on the home page) to ask for the
  // program drawer without lifting state up — the AppShell stays the single
  // owner of drawer state.
  useEffect(() => {
    const handler = () => setIsDrawerOpen(true);
    window.addEventListener(OPEN_PROGRAM_EVENT, handler);
    return () => window.removeEventListener(OPEN_PROGRAM_EVENT, handler);
  }, []);

  // After the i18n restructure every route lives under `/<lang>/...`.
  // Strip the lang segment so module/slug detection sees the same shape
  // it did pre-i18n; the AppShell stays language-agnostic for routing
  // but forwards the active lang to the server-rendered Header (which
  // needs it for aria-labels via getDict).
  const { lang: parsedLang, rest } = stripLangFromPath(pathname);
  const lang = parsedLang ?? DEFAULT_LANG;
  const segments = rest.replace(/^\/+|\/+$/g, '').split('/').filter(Boolean);
  const moduleId = segments[0];
  const lessonSlug = segments[1];

  const currentModule = moduleId
    ? course.modules.find((m) => m.id === moduleId)
    : undefined;
  const currentLesson =
    currentModule && lessonSlug
      ? currentModule.lessons.find((l) => l.slug === lessonSlug)
      : undefined;

  const prev =
    currentModule && currentLesson
      ? getPrevLesson(course, currentModule.id, currentLesson.slug)
      : null;
  const next =
    currentModule && currentLesson
      ? getNextLesson(course, currentModule.id, currentLesson.slug)
      : null;

  const breadcrumbs = (
    <Breadcrumbs
      lang={lang}
      moduleId={currentModule?.id}
      moduleTitle={currentModule?.title}
      lessonTitle={currentLesson?.title}
    />
  );

  const actions = (
    <>
      <ProgressBar total={getTotalLessons(course)} lang={lang} />
      {currentModule && currentLesson ? (
        <HeaderLessonNav
          prev={prev}
          next={next}
          currentModuleId={currentModule.id}
          currentSlug={currentLesson.slug}
        />
      ) : null}
    </>
  );

  // Home and module index pages own their own hero (eyebrow + breadcrumbs +
  // progress card) — the global header would duplicate that chrome. Lesson
  // pages keep the header because they need lesson nav + reading progress.
  const isHome = rest === '/' || segments.length === 0;
  const isModuleIndex = !!moduleId && !lessonSlug;
  const hideHeader = isHome || isModuleIndex;

  return (
    <div className={styles.shell}>
      <Sidebar
        onProgramClick={() => setIsDrawerOpen((prev) => !prev)}
        isProgramOpen={isDrawerOpen}
        repoUrl={course.repoUrl}
      />
      <ProgramDrawer
        course={course}
        currentModuleId={currentModule?.id}
        currentSlug={currentLesson?.slug}
        currentLessonTitle={currentLesson?.title}
        currentLessonIndex={
          currentModule && currentLesson
            ? currentModule.lessons.findIndex((l) => l.slug === currentLesson.slug) + 1
            : undefined
        }
        currentModuleTitle={currentModule?.title}
        isOpen={isDrawerOpen}
        onClose={() => setIsDrawerOpen(false)}
        prev={prev}
        next={next}
        repoUrl={course.repoUrl}
        lang={lang}
        totalLessons={getTotalLessons(course)}
      />
      <div className={styles.body}>
        {hideHeader ? null : <Header lang={lang} breadcrumbs={breadcrumbs} actions={actions} />}
        <main className={styles.main}>{children}</main>
      </div>
    </div>
  );
}
