'use client';

import Link from 'next/link';
import {
  lessonKey,
  PROGRESS_CHANGE_EVENT,
  unmarkCompleted,
} from '@/lib/progress';
import { useLang, useT } from '@/lib/use-i18n';
import styles from './LessonSideMeta.module.css';

type LessonSideMetaProps = {
  moduleId: string;
  moduleTitle: string;
  moduleIndex: number;
  slug: string;
  duration: string;
  tags: string[];
};

export function LessonSideMeta({
  moduleId,
  moduleTitle,
  moduleIndex,
  slug,
  duration,
  tags,
}: LessonSideMetaProps) {
  const key = lessonKey(moduleId, slug);
  const moduleNum = String(moduleIndex).padStart(2, '0');
  const t = useT();
  const lang = useLang();

  const handleUnmark = () => {
    unmarkCompleted(key);
    window.dispatchEvent(new Event(PROGRESS_CHANGE_EVENT));
  };

  return (
    <div
      className={styles.meta}
      data-lesson-key={key}
      // Gate-mark inline script flips data-completed / data-locked on every
      // [data-lesson-key] before hydration. Without this hint React warns
      // about "extra attributes from the server" on this div.
      suppressHydrationWarning
    >
      <div className={styles.row}>
        <span className={styles.key}>{t.moduleMetaKey}</span>
        <Link href={`/${lang}/${moduleId}`} className={styles.value}>
          {moduleNum} · {moduleTitle}
        </Link>
      </div>
      <div className={styles.row}>
        <span className={styles.key}>{t.readingTimeMetaKey}</span>
        <span className={styles.value}>{duration}</span>
      </div>
      {tags.length > 0 && (
        <div className={styles.row}>
          <span className={styles.key}>{t.tagsMetaKey}</span>
          <span className={styles.tags}>
            {tags.map((tag) => (
              <span key={tag} className={styles.tag}>
                #{tag}
              </span>
            ))}
          </span>
        </div>
      )}
      <button
        type="button"
        className={styles.markButton}
        data-show-when-completed
        onClick={handleUnmark}
      >
        {t.markUnread}
      </button>
    </div>
  );
}
