import React from "react";
import styles from "./HighlightTag.module.less";
import cls from "classnames";

import { ReadonlyVal, val, Val } from "value-enhancer";
import { useVal } from "use-value-enhancer";
import { QueryKeyword } from "../store";

const context = React.createContext<ReadonlyVal<readonly Val<QueryKeyword>[]>>(val([]));

export type HighlightTagProps = React.PropsWithChildren<React.HTMLAttributes<HTMLSpanElement> & {
  readonly keyword: string;
}>;

export const HighlightTag: React.FC<HighlightTagProps> = React.memo(({ keyword, children, ...rest }) => {
  const keywords$ = React.useContext(context);
  const keywords = useVal(keywords$);
  const keyword$ = keywords.find(k => k.value.name === keyword);
  if (!keyword$) {
    return null;
  }
  return (
    <HighlightTagHandler
      keyword$={keyword$}
      spanProps={rest}>
      {children}
    </HighlightTagHandler>
  );
});

const HighlightTagHandler: React.FC<React.PropsWithChildren<{
  readonly keyword$: ReadonlyVal<QueryKeyword>;
  readonly spanProps: React.HTMLAttributes<HTMLSpanElement>;
}>> = ({ keyword$, children, spanProps }) => {
  const keyword = useVal(keyword$);
  const className = cls(
    spanProps.className,
    keyword.checked ? styles.highlight : undefined,
  );
  return (
    <span {...spanProps} className={className}>
      {children}
    </span>
  );
};

export type HighlightProviderProps = React.PropsWithChildren<{
  readonly keywords: readonly QueryKeyword[];
}>;

export const HighlightProvider: React.FC<HighlightProviderProps> = ({ keywords, children }) => {
  const keywords$ = React.useMemo(
    () => val<readonly Val<QueryKeyword>[]>([]),
    [],
  );
  React.useEffect(
    () => {
      const originKeywords = keywords$.value;
      const newKeywords: Val<QueryKeyword>[] = [];
      let didCreateNew = false;

      for (const keyword of keywords) {
        let keyword$ = originKeywords.find(k => k.value.name === keyword.name);
        if (keyword$) {
          keyword$.value = keyword;
        } else {
          keyword$ = val(keyword);
          didCreateNew = true;
        }
        newKeywords.push(keyword$);
      }
      if (didCreateNew || originKeywords.length !== newKeywords.length) {
        // 如果只是 checked 本身的更新，则仅推送到元素上，而非改编整体结构令全部都刷新，以提高性能
        keywords$.value = newKeywords;
      }
    },
    [keywords$, keywords],
  );
  return (
    <context.Provider value={keywords$}>
      {children}
    </context.Provider>
  );
};

