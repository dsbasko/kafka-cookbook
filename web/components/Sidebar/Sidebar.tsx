'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { SettingsToggle } from '@/components/SettingsToggle';
import { stripLangFromPath } from '@/lib/lang';
import { useLang, useT } from '@/lib/use-i18n';
import { HomeIcon, ProgramIcon, GitHubIcon } from './icons';
import styles from './Sidebar.module.css';

type SidebarProps = {
  onProgramClick: () => void;
  isProgramOpen: boolean;
  repoUrl: string;
};

export function Sidebar({ onProgramClick, isProgramOpen, repoUrl }: SidebarProps) {
  const pathname = usePathname() ?? '/';
  const lang = useLang();
  const { rest } = stripLangFromPath(pathname);
  const isHome = rest === '/' || pathname === '/';
  const t = useT();
  return (
    <aside className={styles.sidebar} aria-label={t.sidebarLabel}>
      <nav className={styles.nav} aria-label={t.navMainLabel}>
        <Link
          href={`/${lang}`}
          className={styles.button}
          aria-label={t.home}
          title={t.home}
          aria-current={isHome ? 'page' : undefined}
        >
          <HomeIcon />
        </Link>
        <button
          type="button"
          className={styles.button}
          aria-label={t.programCourse}
          title={t.programCourse}
          aria-haspopup="dialog"
          aria-expanded={isProgramOpen}
          onClick={onProgramClick}
        >
          <ProgramIcon />
        </button>
      </nav>

      <div className={styles.footer}>
        <SettingsToggle repoUrl={repoUrl} />
        <a
          className={styles.button}
          href={repoUrl}
          target="_blank"
          rel="noreferrer noopener"
          aria-label={t.githubRepo}
          title={t.githubRepo}
        >
          <GitHubIcon />
        </a>
      </div>
    </aside>
  );
}
