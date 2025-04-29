import React from "react";
import styles from "./QueryPage.module.less";

import { Tag, Empty, Skeleton, Input, List, InputNumber } from "antd";
import { useVal } from "use-value-enhancer";
import { useSearchParams } from "react-router-dom";
import { PDFPageItem, QueryItem } from "../store";
import { context } from "./StoreContext";
import { PDFTagLink } from "./Link";
import { HighlightProvider } from "./HighlightTag";
import { Text } from "./Text";

const { Search } = Input;

export const QueryPage: React.FC<{}> = () => {
  const store = React.useContext(context).queryStore;
  const isQuerying = useVal(store.$.isQuerying);
  const items = useVal(store.$.items);
  let tailView: React.ReactNode = null;

  if (isQuerying) {
    tailView = <Skeleton active />;
  } else if (items) {
    tailView = (
      <ResultDisplay items={items} />
    );
  }
  return (
    <div className={styles.root}>
      <QueryBox />
      {tailView}
    </div>
  );
};

const QueryBox: React.FC<{}> = () => {
  const store = React.useContext(context).queryStore;
  const [searchParams, setSearchParams] = useSearchParams();
  const resultsLimit = useVal(store.$.resultsLimit);
  const isValidResultsLimit = resultsLimit !== null;
  const onResultsLimitChanged = React.useCallback(
    (value: number | null) => store.$.resultsLimit.set(value),
    [store],
  );
  let query: string | null | undefined = searchParams.get("query");

  React.useEffect(
    () => queryIfNeed(query, resultsLimit),
    [store],
  );
  const queryIfNeed = React.useCallback(
    (query: string | null | undefined, resultsLimit: number | null) => {
      if (resultsLimit !== null) {
        if (typeof query !== "string" || query.trim() === "") {
          store.cleanQuery();
        } else {
          store.query(query, resultsLimit);
        }
      }
    },
    [store],
  );
  const onSearch = React.useCallback(
    (query: string) => {
      if (query.trim() === "") {
        setSearchParams({});
      } else {
        setSearchParams({ query });
      }
      queryIfNeed(query, resultsLimit);
    },
    [setSearchParams, resultsLimit],
  );
  return (
    <div className={styles["query-box"]}>
      <Search
        className={styles["query-searcher"]}
        placeholder="输入你要搜索的内容"
        defaultValue={query ?? ""}
        allowClear
        disabled={!isValidResultsLimit}
        onSearch={onSearch} />
      <InputNumber
        className={styles["query-limit"]}
        addonBefore="前"
        addonAfter="个结果"
        min={1}
        max={100}
        value={resultsLimit}
        status={isValidResultsLimit ? undefined : "warning"}
        onChange={onResultsLimitChanged} />
    </div>
  );
};

type ResultDisplayProps = {
  readonly items: readonly QueryItem[];
};

const ResultDisplay: React.FC<ResultDisplayProps> = ({ items }) => {
  const store = React.useContext(context).queryStore;
  const keywords = useVal(store.$.keywords);
  return <>
    <Keywords />
    <HighlightProvider keywords={keywords}>
      <div className={styles["query-result-box"]}>
        {items.length === 0 && (
          <Empty
            className={styles.empty}
            description="没有搜索到内容" />
        )}
        {items.map((item, index) => {
          if (!("content" in item)) {
            // TODO: 对 PDF Metadata 本身的搜索
            return null;
          }
          return (
            <PDFPageCard key={`${index}`} item={item} />
          );
        })}
      </div>
    </HighlightProvider>
  </>;
};

const Keywords: React.FC<{}> = () => {
  const store = React.useContext(context).queryStore;
  const keywords = useVal(store.$.keywords);
  const onChangeTag = React.useCallback(
    (name: string, checked: boolean) => store.checkTag(name, checked),
    [store],
  );
  return (
    <div className={styles["keywords-bar"]}>
      <div className={styles["keywords-title"]}>
        关键词：
      </div>
      <div className={styles["keywords"]}>
        {keywords.map((keyword, index) => (
          <Tag.CheckableTag
            key={`${index}`}
            checked={keyword.checked}
            onChange={checked => onChangeTag(keyword.name, checked)} >
            {keyword.name}
          </Tag.CheckableTag>
        ))}
      </div>
    </div>
  );
};

type PDFPageCardProps = {
  readonly item: PDFPageItem;
};

const PDFPageCard: React.FC<PDFPageCardProps> = ({ item }) => {
  const { distance, pdf_files, content, segments } = item;
  return (
    <div className={styles["pdf-page-card"]}>
      <List>
        <List.Item>
          <List.Item.Meta
            title="距离"
            description={distance} />
          <List.Item.Meta
            title="匹配片段"
            description={segments.length} />
        </List.Item>
        <List.Item.Meta
          title="文件"
          description={
            pdf_files.map((pdf, index) => (
              <PDFTagLink
                key={`${index}`}
                scope={pdf.scope}
                scopePath={pdf.path}
                devicePath={pdf.device_path}
                page={pdf.page_index} />
            ))
          } />
      </List>
      <Text
        className={styles.text}
        content={content}
        segments={segments} />
    </div>
  );
};