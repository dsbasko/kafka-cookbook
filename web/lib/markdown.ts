import { createElement, type ReactElement } from 'react';
import { Fragment, jsx, jsxs } from 'react/jsx-runtime';
import { unified, type Plugin } from 'unified';
import remarkParse from 'remark-parse';
import remarkGfm from 'remark-gfm';
import remarkRehype from 'remark-rehype';
import rehypeSlug from 'rehype-slug';
import rehypeAutolinkHeadings from 'rehype-autolink-headings';
import rehypePrettyCode from 'rehype-pretty-code';
import { toJsxRuntime } from 'hast-util-to-jsx-runtime';
import type { Element, Root as HastRoot } from 'hast';
import { remarkAlert } from 'remark-github-blockquote-alert';
import remarkLessonImages from './mdx-plugins/remark-lesson-images';
import remarkLinkRewrite from './mdx-plugins/remark-link-rewrite';
import rehypeCallout from './mdx-plugins/rehype-callout';
import type { Course } from './course';
import { DEFAULT_LANG, type Lang } from './lang';
import { LessonAwareLink } from '@/components/LessonAwareLink';
import { makeMarkdownAside, makeMarkdownFigure } from './markdown-components';
import { extractToc, type TocEntry } from './extract-toc';

export interface RenderLessonMarkdownOptions {
  moduleId: string;
  slug: string;
  basePath: string;
  course: Course;
  /**
   * Active route language. Determines the `/<lang>/` prefix the rewriter
   * stamps on emitted site URLs. Defaults to {@link DEFAULT_LANG}.
   */
  lang?: Lang;
  /**
   * Language of the README being rendered. When an EN route falls back to the
   * RU README this differs from {@link lang}: the URL prefix stays `/en/` but
   * relative sibling links inside the body anchor at `i18n/ru/`. Defaults to
   * {@link lang}.
   */
  sourceLang?: Lang;
}

export interface RenderLessonMarkdownResult {
  content: ReactElement;
  toc: TocEntry[];
}

const PRETTY_CODE_OPTIONS = {
  theme: { light: 'github-light', dark: 'night-owl' },
  keepBackground: false,
  defaultLang: 'plaintext',
} as const;

/**
 * The lesson page already renders the `course.yaml` title as the page-level
 * `<h1>` via `LessonLayout`. Lecture READMEs also start with their own `# Title`
 * heading, which would produce a second `<h1>` in the article body — bad for
 * the document outline, screen-reader navigation and SEO. Strip the leading
 * `<h1>` from the lesson-content tree so the remaining `h2`/`h3` headings sit
 * beneath the page H1 cleanly.
 */
const rehypeStripLeadingH1: Plugin<[], HastRoot> = () => {
  return (tree) => {
    for (let i = 0; i < tree.children.length; i += 1) {
      const child = tree.children[i];
      if (child.type !== 'element') continue;
      if (child.tagName === 'h1') {
        tree.children.splice(i, 1);
      }
      return;
    }
  };
};

/**
 * rehype-pretty-code stamps `data-language` on the inner <pre>/<code>, but our
 * CodeBlock wrapper renders at the <figure> level. Lift the language up so the
 * figure component can show it without inspecting children.
 */
const rehypeLiftCodeBlockLanguage: Plugin<[], HastRoot> = () => {
  return (tree) => {
    walk(tree, (node) => {
      if (node.tagName !== 'figure') return;
      const props = node.properties;
      if (!props || !('data-rehype-pretty-code-figure' in props)) return;
      for (const child of node.children) {
        if (child.type !== 'element' || child.tagName !== 'pre') continue;
        const lang = (child.properties as Record<string, unknown> | undefined)?.['data-language'];
        if (typeof lang === 'string') {
          (props as Record<string, unknown>)['data-language'] = lang;
        }
        return;
      }
    });
  };
};

function walk(node: HastRoot | Element, visit: (el: Element) => void) {
  for (const child of node.children) {
    if ((child as Element).type === 'element') {
      visit(child as Element);
      walk(child as Element, visit);
    }
  }
}

function hasAnchorDescendant(node: Element): boolean {
  for (const child of node.children) {
    if (child.type !== 'element') continue;
    if (child.tagName === 'a') return true;
    if (hasAnchorDescendant(child)) return true;
  }
  return false;
}

export async function renderLessonMarkdown(
  source: string,
  options: RenderLessonMarkdownOptions,
): Promise<RenderLessonMarkdownResult> {
  const processor = unified()
    .use(remarkParse)
    .use(remarkGfm)
    .use(remarkAlert)
    .use(remarkLessonImages, {
      moduleId: options.moduleId,
      slug: options.slug,
      basePath: options.basePath,
    })
    .use(remarkLinkRewrite, {
      moduleId: options.moduleId,
      slug: options.slug,
      basePath: options.basePath,
      course: options.course,
      lang: options.lang ?? DEFAULT_LANG,
      sourceLang: options.sourceLang ?? options.lang ?? DEFAULT_LANG,
    })
    .use(remarkRehype, { allowDangerousHtml: false })
    .use(rehypeStripLeadingH1)
    .use(rehypeCallout)
    .use(rehypeSlug)
    .use(rehypeAutolinkHeadings, {
      behavior: 'wrap',
      properties: { className: ['heading-anchor'] },
      // Skip headings that already contain an <a> (markdown links inside the
      // heading text). Wrapping such a heading would emit invalid HTML
      // (<a><h2>...<a>...</a>...</h2></a>) and trigger a hydration failure.
      test: (node) => !hasAnchorDescendant(node),
    })
    .use(rehypePrettyCode, PRETTY_CODE_OPTIONS)
    .use(rehypeLiftCodeBlockLanguage);

  const mdast = processor.parse(source);
  const hast = (await processor.run(mdast)) as HastRoot;

  const toc = extractToc(hast);

  // Wrap LessonAwareLink in a closure so it learns the build's basePath at
  // render time without needing a React context (renderLessonMarkdown runs
  // server-side; GateContext is client-only). The wrapper passes basePath
  // down so the link can derive data-lesson-key from href on the server,
  // which is what the inline gate-mark script needs to find at first paint.
  const basePath = options.basePath;
  const lang = options.lang ?? DEFAULT_LANG;
  const LessonLink = (props: Record<string, unknown>) =>
    createElement(LessonAwareLink, {
      basePath,
      ...(props as Record<string, unknown>),
    } as never);

  const content = toJsxRuntime(hast, {
    Fragment,
    jsx: jsx as never,
    jsxs: jsxs as never,
    components: {
      a: LessonLink as never,
      figure: makeMarkdownFigure(lang) as never,
      aside: makeMarkdownAside(lang) as never,
    },
  }) as ReactElement;

  return { content, toc };
}
